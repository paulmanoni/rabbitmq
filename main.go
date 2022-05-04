package rabbitmq

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"os"
)

type Service struct {
	serviceName string
	username    string
	password    string
	addr        string
}

func (service *Service) url() string {
	return "amqp://" + service.username + ":" + service.password + "@" + service.addr + "/"
}

type Session struct {
	name            string
	logger          *log.Logger
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}

func (session *Session) changeChannel(channel *amqp.Channel) {
	session.channel = channel
}

func (session *Session) changeConnection(connection *amqp.Connection) {
	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

func (session *Session) Connect(service Service) (*amqp.Connection, error) {
	connect, err := amqp.Dial(service.url())
	if err != nil {
		session.logger.Printf("Failed to connect to RabbitMQ: %s", err)
		return nil, err
	}
	session.changeConnection(connect)
	session.logger.Printf("Connected to RabbitMQ: %s", service.addr)

	return connect, nil
}

func (session *Session) Init(conn *amqp.Connection, onWait bool) error {
	channel, err := conn.Channel()
	if err != nil {
		session.logger.Printf("Failed to open a channel: %s", err)
	}
	err = channel.Confirm(onWait)
	if err != nil {
		return err
	}

	_, err = channel.QueueDeclare(session.name, false, false, false, false, nil)
	if err != nil {
		return err
	}

	session.changeChannel(channel)
	session.isReady = true

	return nil
}

func (session *Session) UnsafePush(data []byte) error {
	if !session.isReady {
		return errors.New("failed to push push: not connected")
	}
	return session.channel.Publish(
		"",           // Exchange
		session.name, // Routing key
		false,        // Mandatory
		false,        // Immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         data,
		},
	)
}

func (session *Session) Push(data []byte) error {
	if !session.isReady {
		session.logger.Printf("Session is not ready")
		return errors.New("failed to push push: not connected")
	}

	for {
		err := session.UnsafePush(data)
		if err != nil {
			session.logger.Println("Push failed. Retrying...")
		}
		select {
		case <-session.notifyConnClose:
			session.logger.Printf("Connection closed")
			return nil
		case <-session.notifyChanClose:
			session.logger.Printf("Channel closed")
			return nil
		case confirm := <-session.notifyConfirm:
			if confirm.Ack {
				session.logger.Printf("Message confirmed")
			}
			return nil
		case <-session.done:
			return nil
		}
	}
}

func (session *Session) Stream() (<-chan amqp.Delivery, error) {
	return session.channel.Consume(
		session.name, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // no-local
		false,        // no-wait
		false,        // exclusive
		nil,
	)
}

func (session *Session) Close() error {
	if !session.isReady {
		return errors.New("failed to close session: not connected")
	}
	err := session.channel.Close()
	if err != nil {
		return err
	}
	err = session.connection.Close()
	if err != nil {
		return err
	}
	close(session.done)
	session.isReady = false
	session.logger.Printf("Close Connection with RabbitMQ")
	return nil
}

func NewInit(sessionName string, serviceName string, username string, password string, address string, onWait bool) Session {
	session := Session{
		name:   sessionName,
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}

	connect, _ := session.Connect(Service{
		serviceName: serviceName,
		username:    username,
		password:    password,
		addr:        address,
	})
	err := session.Init(connect, onWait)
	if err != nil {
		return Session{}
	}
	return session
}
