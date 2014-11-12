package main

import (
	"./JsonMessage"
	"./config"
	"./logger"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"gopkg.in/gomail.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"strings"
	"time"
)

func SendEmail(to, jsonfile string, split int) {
	msg := gomail.NewMessage()
	msg.SetHeader("From", config.EMAIL_EXPORT_FROM_ADDRESS)
	msg.SetHeader("To", to)
	// msg.SetAddressHeader("Cc", "hhj007v@gmail.com", "Dan") //참조
	msg.SetHeader("Subject", "Hello!")
	msg.SetBody("text/html", "")

	f, err := gomail.OpenFile(jsonfile)
	if err != nil {
		panic(err)
	}

	h := strings.Split(f.Name, ".")
	h[0] += string(split)
	fmt.Println(h[0])
	f.Name = h[0] + "." + h[1]
	msg.Attach(f)

	mailer := gomail.NewMailer("smtp.gmail.com", config.EMAIL_EXPORT_FROM_ADDRESS, config.EMAIL_EXPORT_FROM_PASSWD, 587)
	if err := mailer.Send(msg); err != nil {
		panic(err)
	}
}

func main() {
	//create logger
	logger.CreateLogger("logfile")
	defer logger.DropLogger()

	mongodb := config.MONGODB_HOST + ":" + config.MONGODB_PORT
	session, Mongoerr := mgo.Dial(mongodb)
	if Mongoerr != nil {
		panic(Mongoerr)
	}
	defer session.Close()

	//connect to RabbitMQ
	rabbitmq := "amqp://" + config.RABBITMQ_USER + ":" + config.RABBITMQ_PASS + "@" + config.RABBITMQ_HOST + ":" + config.RABBITMQ_PORT
	conn, MQerr := amqp.Dial(rabbitmq)
	if MQerr != nil {
		panic(MQerr)
	}
	defer conn.Close()

	ch, Channelerr := conn.Channel()
	logger.FailOnError(Channelerr, "Failed to open a channel")
	defer ch.Close()

	msgs, Consumeerr := ch.Consume(
		"export", // queue
		"",       // consumer   	consumer에 대한 식별자를 지정합니다. consumer tag는 로컬에 channel이므로, 두 클라이언트는 동일한 consumer tag를 사용할 수있다.
		false,    // autoAck    	false는 명시적 Ack를 해줘야 메시지가 삭제되고 true는 메시지를 빼면 바로 삭제
		false,    // exclusive	현재 connection에만 액세스 할 수 있으며, 연결이 종료 할 때 Queue가 삭제됩니다.
		false,    // noLocal    	필드가 설정되는 경우 서버는이를 published 연결로 메시지를 전송하지 않을 것입니다.
		false,    // noWait		설정하면, 서버는 Method에 응답하지 않습니다. 클라이언트는 응답 Method를 기다릴 것이다. 서버가 Method를 완료 할 수 없을 경우는 채널 또는 연결 예외를 발생시킬 것입니다.
		nil,      // arguments	일부 브로커를 사용하여 메시지의 TTL과 같은 추가 기능을 구현하기 위해 사용된다.
	)
	logger.FailOnError(Consumeerr, "Failed to register a consumer")

	forever := make(chan bool)

	// for i := 0; i < 1; i++ {
	// 	go func() {
	for d := range msgs {

		var m JsonMessage.MongoExport
		if err := json.Unmarshal([]byte(d.Body), &m); err != nil {
			logger.FailOnError(err, "Failed to json.Unmarshal")
			continue
		}
		CollectionName := m.Collection
		//MongoDB set
		c := session.DB(config.NAMESPACE + ":" + "haru").C(CollectionName)
		var file *os.File
		var Openerr error
		file, Openerr = os.OpenFile(CollectionName+".json", os.O_WRONLY|os.O_CREATE, 0640)
		if Openerr != nil {
			fmt.Println(Openerr)
		}
		var split int = 1
		var length int = 0
		var result []map[string]interface{}
		var max_created_date_from_last_result interface{} = ""

		if count, err := c.Find(nil).Count(); err != nil {
			fmt.Println(err)
			return
		} else {
			fmt.Println(count)
			for i := 0; i < count; i += length {
				t0 := time.Now()
				c.Find(bson.M{"_id": bson.M{"$gt": max_created_date_from_last_result}}).Limit(5000).All(&result)
				t1 := time.Now()
				fmt.Printf("The call took %v to run.\n", t1.Sub(t0))
				for _, v := range result {
					msg, _ := json.Marshal(v)
					file.WriteString(string(msg) + "\n")
				}
				length = len(result)
				fmt.Println(count - i)
				fileinfo, _ := file.Stat()
				if fileinfo.Size() > 23000000 {
					if CloseErr := file.Close(); CloseErr != nil {
						fmt.Println("true")
					} else {
						fmt.Println("false")
						SendEmail(m.Address, CollectionName+".json", split)
						split++
						file, Openerr = os.OpenFile(CollectionName+".json", os.O_WRONLY|os.O_CREATE, 0640)
						if Openerr != nil {
							fmt.Println(Openerr)
						}
					}
				}
				fmt.Println("file size: ", fileinfo.Size())
				max_created_date_from_last_result = result[(length - 1)]["_id"]
			}
			SendEmail(m.Address, CollectionName+".json", split)
			if err := os.Remove(CollectionName + ".json"); err != nil {
				fmt.Println(err)
				return
			}
		}

		//RabbitMQ Message delete
		d.Ack(false)
	}

	// 	}()
	// }
	<-forever

	// err := iter.All(&result)
	// fmt.Println(result)

	// mylogger = log.New(io.MultiWriter(logf), "worker: ", )
	// mylogger.Println("goes to logf")

	// if err != nil {
	// 	fmt.Println(err)
	// }
	// iter.All(&result)
	// fmt.Println(len(result))
	// for i, v := range result {
	// 	fmt.Println(i+1, v)

	// }

	// fmt.Println(iter.Timeout())
	// fmt.Println(iter.Next(result))

	// for iter.Next(&result) {
	// 	fmt.Println(result)
	// }
	// if err := iter.Close(); err != nil {
	// 	fmt.Println(err)
	// }
	// lastpage = c.Find().sort(bson.M{"_id": 1}).limit(3)
	// for lastpage {

	// }

	// for d := range msgs {

	// 	c.Find(bson.M{"_id": bson.M{"$gt": "max_created_date_from_last_result"}}).limit(1)
	// 	if err := c.Find(bson.M{}).Sort("-time").All(&notices); err != nil {
	// 		r.JSON(http.StatusNotFound, err)
	// 		return
	// 	}
	// }
	// 	}()
	// }

	// <-forever
	fmt.Printf("Worker is Dead.")

	os.Exit(0)
}
