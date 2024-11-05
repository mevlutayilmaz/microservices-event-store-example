using EventStore.Client;
using System.Text.Json;

#region Event Store Kullanımı

#region Bağlantı Sağlama
//string connectionString = "esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false";
//var settings = EventStoreClientSettings.Create(connectionString);
//var client = new EventStoreClient(settings);
#endregion

#region Event Oluşturma ve Stream'e Gönderme
//OrderPlacedEvent orderPlacedEvent = new()
//{
//    OrderId = 1,
//    TotalAmount = 800
//};

//for (int i = 0; i < 20; i++)
//{
//    EventData eventData = new(
//        eventId: Uuid.NewUuid(),
//        type: orderPlacedEvent.GetType().Name,
//        data: JsonSerializer.SerializeToUtf8Bytes(orderPlacedEvent));

//    await client.AppendToStreamAsync(
//        streamName: "order-stream",
//        expectedState: StreamState.Any,
//        eventData: [eventData]);
//}
#endregion

#region Stream Okuma
//var events = client.ReadStreamAsync(
//    direction: Direction.Forwards, 
//    streamName: "order-stream", 
//    revision: StreamPosition.Start);

//var datas = await events.ToListAsync();
//Console.WriteLine();
#endregion

#region Event Store'da Stream Subscription
//await client.SubscribeToStreamAsync(
//    streamName: "order-stream",
//    start: FromStream.Start,
//    eventAppeared: async (streamSubscription, resolvedEvent, cancellationToken) =>
//    {
//        OrderPlacedEvent @event = JsonSerializer.Deserialize<OrderPlacedEvent>(resolvedEvent.Event.Data.ToArray());
//        Console.WriteLine(JsonSerializer.Serialize(@event));
//    },
//    subscriptionDropped: (streamSubscription, subscriptionDroppedReason, esception) => Console.WriteLine("Disconnected"));
#endregion

//class OrderPlacedEvent
//{
//    public int OrderId { get; set; }
//    public int TotalAmount { get; set; }
//}
#endregion

#region Bakiye Senaryosu

EventStoreService eventStoreService = new();

#region Event Oluşturma ve Stream'e Yollama
AccountCreatedEvent accountCreatedEvent = new()
{
    AccountId = "12345",
    CostumerId = "98765",
    StartBalance = 0,
    Date = DateTime.UtcNow.Date
};
MoneyDepositedEvent moneyDepositedEvent1 = new()
{
    AccountId = "12345",
    Amount = 1000,
    Date = DateTime.UtcNow.Date
};
MoneyDepositedEvent moneyDepositedEvent2 = new()
{
    AccountId = "12345",
    Amount = 500,
    Date = DateTime.UtcNow.Date
};
MoneyWithdrawnEvent moneyWithdrawnEvent = new()
{
    AccountId = "12345",
    Amount = 200,
    Date = DateTime.UtcNow.Date
};
MoneyDepositedEvent moneyDepositedEvent3 = new()
{
    AccountId = "12345",
    Amount = 50,
    Date = DateTime.UtcNow.Date
};
MoneyTransferredEvent moneyTransferredEvent1 = new()
{
    AccountId = "12345",
    Amount = 250,
    Date = DateTime.UtcNow.Date
};
MoneyTransferredEvent moneyTransferredEvent2 = new()
{
    AccountId = "12345",
    Amount = 150,
    Date = DateTime.UtcNow.Date
};
MoneyDepositedEvent moneyDepositedEvent4 = new()
{
    AccountId = "12345",
    Amount = 2000,
    Date = DateTime.UtcNow.Date
};

//await eventStoreService.AppendToStreamAsyn(
//    streamName: $"consumer-{accountCreatedEvent.CostumerId}-stream",
//    eventData: [
//        eventStoreService.GenerateEventData(accountCreatedEvent),
//        eventStoreService.GenerateEventData(moneyDepositedEvent1),
//        eventStoreService.GenerateEventData(moneyDepositedEvent2),
//        eventStoreService.GenerateEventData(moneyWithdrawnEvent),
//        eventStoreService.GenerateEventData(moneyDepositedEvent3),
//        eventStoreService.GenerateEventData(moneyTransferredEvent1),
//        eventStoreService.GenerateEventData(moneyTransferredEvent2),
//        eventStoreService.GenerateEventData(moneyDepositedEvent4),
//        ]);
#endregion

BalanceInfo balanceInfo = new();
await eventStoreService.SubscribeToStreamAsync(
    streamName: $"consumer-{accountCreatedEvent.CostumerId}-stream",
    eventAppeared: async (ss, re, ct) =>
    {
        string eventType = re.Event.EventType;
        object @event = JsonSerializer.Deserialize(re.Event.Data.ToArray(), Type.GetType(eventType));

        switch (@event)
        {
            case AccountCreatedEvent e:
                balanceInfo.AccountId = e.AccountId;
                balanceInfo.Balance = e.StartBalance;
                break;
            case MoneyDepositedEvent e:
                balanceInfo.Balance += e.Amount;
                break;
            case MoneyWithdrawnEvent e:
                balanceInfo.Balance -= e.Amount;
                break;
            case MoneyTransferredEvent e:
                balanceInfo.Balance -= e.Amount;
                break;
        }

        Console.WriteLine("********Balance********");
        Console.WriteLine(JsonSerializer.Serialize(balanceInfo));
        Console.WriteLine("********Balance********");
        Console.WriteLine();
    });

Console.Read();

class EventStoreService
{
    EventStoreClientSettings GetEventStoreClientSettings(string connectionStrin = "esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false") => EventStoreClientSettings.Create(connectionStrin);

    EventStoreClient Client { get => new EventStoreClient(GetEventStoreClientSettings()); }
    public async Task AppendToStreamAsyn(string streamName, IEnumerable<EventData> eventData)
        => await Client.AppendToStreamAsync(
            streamName: streamName,
            expectedState: StreamState.Any,
            eventData: eventData);

    public EventData GenerateEventData(object @event)
        => new EventData(
            eventId: Uuid.NewUuid(),
            type: @event.GetType().Name,
            data: JsonSerializer.SerializeToUtf8Bytes(@event));

    public async Task SubscribeToStreamAsync(string streamName, Func<StreamSubscription, ResolvedEvent, CancellationToken, Task> eventAppeared)
        => await Client.SubscribeToStreamAsync(
            streamName: streamName,
            start: FromStream.Start,
            eventAppeared: eventAppeared,
            subscriptionDropped: (streamSubscription, subscriptionDroppedReason, exception) => Console.WriteLine("Disconnected!"));
}

class BalanceInfo
{
    public string AccountId { get; set; }
    public int Balance { get; set; }
}

class AccountCreatedEvent
{
    public string AccountId { get; set; }
    public string CostumerId { get; set; }
    public int StartBalance { get; set; }
    public DateTime Date { get; set; }
}

class MoneyDepositedEvent
{
    public string AccountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}

class MoneyWithdrawnEvent
{
    public string AccountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}

class MoneyTransferredEvent
{
    public string AccountId { get; set; }
    public string TargetAccountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}

#endregion


