# easy-go

[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)][godev]
![GitHub](https://img.shields.io/github/license/rusfort/easy-go)
[![Telegram](https://img.shields.io/static/v1?label=telegram&message=rusfort&color=00add8)](https://t.me/rusfort)

[godev]: https://pkg.go.dev/github.com/rusfort/easy-go


## Http request

RequestPost - 

Пример использования:


```go
res, err := eg.RequestPost(ctx, c.apiEndpoint+endpoint, bearerToken,
	req, nil, false,
)
if err != nil {
	if errors.Is(err, eg.ErrAuthFailed) {
		return nil, ErrNeedRefreshToken
	}

	log.Printf("SendOzonRequest: RequestPost: %s", err.Error())

	var data ozerror.OzonError
	errErr := json.Unmarshal(res, &data)
	if errErr != nil {
		log.Printf("Failed to Unmarshal error: %s", errErr.Error())
		return nil, fmt.Errorf("RequestPost: %w", err)
	}

	return nil, fmt.Errorf("RequestPost: %w [ERR_MESSAGE: %s]", err, data.Message)
}
```


## Rate limited Queue

Queue - автоматическая очередь с обратной связью и рейт-лимитом (запросы не будут выполняться чаще заданного времени)

Пример использования:


```go
func New(cfg *config.Config) *Connector {
	return &Connector{
		q:             eg.NewRateLimitedQueue(cfg.OzonConfig.CoolingTimeMillis),
		apiEndpoint:   cfg.OzonConfig.Endpoint,
		authEndpoint:  cfg.AuthConfig.Endpoint.AuthURL,
		tokenEndpoint: cfg.AuthConfig.Endpoint.TokenURL,
	}
}

// .....

func (c *Connector) DeliveryCheck(ctx context.Context, instance, clientPhone, bearerToken string) (bool, error) {
	log.Printf("sending delivery check request")
	out := make(chan *deliverycheck.Response)
	errOut := make(chan error)
	eg.PushToQueue(
		c.q,
		instance,
		func() (any, error) {
			res, err := eg.RequestPost(ctx, c.apiEndpoint+deliverycheck.Endpoint, bearerToken,
				&deliverycheck.Request{
					ClientPhone: clientPhone,
				}, nil,
				true,
			)
			if err != nil {
				if errors.Is(err, eg.ErrAuthFailed) {
					return nil, ErrNeedRefreshToken
				}

				var data ozerror.OzonError
				errErr := json.Unmarshal(res, &data)
				if errErr != nil {
					log.Printf("Failed to Unmarshal error: %s", errErr.Error())
				}

				return nil, fmt.Errorf("RequestPost: %w [ERR_MESSAGE: %s]", err, data.Message)
			}

			var data deliverycheck.Response
			err = json.Unmarshal(res, &data)
			if err != nil {
				return nil, fmt.Errorf("Unmarshal: %w", err)
			}

			return &data, nil
		},
		out,
		errOut,
	)

	log.Printf("send delivery check request")

	r := <-out

	err := <-errOut
	if err != nil {
		return false, fmt.Errorf("err: %w", err)
	}

	log.Printf("got delivery check response: %v", r)

	if r == nil {
		return false, fmt.Errorf("got nil response")
	}

	return r.IsPossible, nil
}
```


## Server + S2S

Server - пример использования:

```go
func Run(cfg *config.Config) {
	handlers := &Handlers{
		s: service.New(cfg),
	}

	eg.NewServer().
		HandleRawAll(eg.RawHandleMap{
			"/api/v1/app/callback": handlers.a.HandleCallback,
			"/api/v1/app/login":    handlers.a.HandleLogin,
			"/api/v1/app/link":     handlers.a.HandleLink,
		}).
		HandleAll(eg.HandleMap{
			"/health":                   handlers.Health,
			"/api/v1/token/generate":    handlers.GenerateToken,
			"/api/v1/token/save":        handlers.SaveToken,
		}).
		HandleAllWithS2S(eg.HandleMap{
			models.MethodReceive:        handlers.ReceiveNotification,
			"/api/v1/notification/init": handlers.InitializeSellersSubscriptions,
		}).
		HandleAll(handlers.handleOzonMethods()).
		Start()
}

func (serv *Handlers) handleOzonMethods() eg.HandleMap {
	hm := make(eg.HandleMap, len(models.OzonMethodMap))
	for method, ozonMethod := range models.OzonMethodMap {
		hm[method] = serv.Ozon(ozonMethod)
	}
	return hm
}

```


## Kafka

Producer - пример использования:

```go

producer := eg.NewKafkaProducer(
	os.Getenv("KAFKA_BROKERS"), 
	os.Getenv("KAFKA_USER"), 
	os.Getenv("KAFKA_PASSWORD"),
)

// ...

testValue := TestStruct{
	MVDrive: "platform",
	By:      "Nikolai Kozakov"
}

err := producer.Produce(os.Getenv("KAFKA_TEST_TOPIC"), uuid.NewString(), testValue)
if err != nil {
	return fmt.Errorf("Produce: %w", err)
}

```

Consumer - пример использования:

```go

consumer := eg.NewKafkaConsumer(
	os.Getenv("KAFKA_BROKERS"), 
	os.Getenv("KAFKA_GROUP_ID"),
	os.Getenv("KAFKA_USER"), 
	os.Getenv("KAFKA_PASSWORD"),
)

// ...

testSource, err := consumer.Consume(ctx, os.Getenv("KAFKA_TEST_TOPIC"))
if err != nil {
	return fmt.Errorf("Consume: %w", err)
}

go func(){
	for message := range testSource {
		log.Printf("got message: key = %v, value = %v", message.Key, message.Value)
	}
}()

```
