using System;
using System.Data;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Server;
using Newtonsoft.Json;

class Program
{
    static async Task Main(string[] args)
    {
        var broker = "broker.hivemq.com";
        var topic = "/mkstat/64";
        var payload = "Hello, MQTT! My name is Kim Hung";
        var username = "mkstat";
        var password = "HgMedia@123";
        var clientId = "790b4ae857094e09b0d2ebb722bc76a2";

        //await PublisherAsync(broker, topic, payload, username, password, clientId);
        //await SubscribeAsync(broker, topic, username, password, clientId);

        var data = new SensorData
        {
            Id = 11,
            Packet_no = 126,
            Temperature = 30,
            Humidity = 60,
            Tds = 1100,
            PH = 50,
        };
        var jsonData = JsonConvert.SerializeObject(data);
        await PublisherAsync(broker, topic, jsonData, username, password, clientId);

        Console.ReadKey();
    }

    private static async Task PublisherAsync(string broker, string topic, string payload, string username, string password, string clientId)
    {
        var factory = new MqttFactory();
        var mqttClient = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(broker)
            .WithCredentials(username, password)
            .WithCredentials(clientId)
            .Build();
        await mqttClient.ConnectAsync(options, CancellationToken.None);
        await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic(topic).Build());
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();
        await mqttClient.PublishAsync(message, CancellationToken.None);
        await mqttClient.DisconnectAsync();
    }

    private static async Task SubscribeAsync(string broker, string topic, string username, string password, string clientId)
    {
        var factory = new MqttFactory();
        var mqttClient = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(broker)
            .WithCredentials(username, password)
            .WithCredentials(clientId)
            .Build();
        await mqttClient.ConnectAsync(options, CancellationToken.None);
        await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic(topic).Build());
        mqttClient.ApplicationMessageReceivedAsync += async e =>
        {
            if (e.ApplicationMessage.Topic == topic)
            {
                try
                {
                    var sensorData = JsonConvert.DeserializeObject<SensorData>(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));

                    if (sensorData != null)
                    {
                        // Dữ liệu nhận được có định dạng đúng
                        Console.WriteLine($"Received data: Id={sensorData.Id}, Packet_no={sensorData.Packet_no}, Temperature={sensorData.Temperature}, Humidity={sensorData.Humidity}, Tds={sensorData.Tds}, PH={sensorData.PH}");
                    }
                    else
                    {
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Received message on topic '{0}': {1}", e.ApplicationMessage.Topic, Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                }
            }
        };
        Console.ReadLine();
        await mqttClient.DisconnectAsync();
    }
}
class SensorData
{
    public int Id { get; set; }
    public int Packet_no { get; set; }
    public int Temperature { get; set; }
    public int Humidity { get; set; }
    public int Tds { get; set; }
    public int PH { get; set; }

}
