namespace SampleModule
{
    using System;
    using System.Runtime.Loader;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;

    class Program
    {
        static int counter;
        static ModuleClient ioTHubModuleClient;

        static void Main(string[] _)
        {
            Init().Wait();

            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();

            SendTempHub(cts.Token).Wait();

            // Wait until the app unloads or is cancelled
            WhenCancelled(cts.Token).Wait();
        }

        public struct Data
        {
            public Data(double temperature, double humidity)
            {
                Temperature = temperature;
                Humidity = humidity;
            }

            public readonly double Temperature;
            public readonly double Humidity;
        }

        static async Task SendTempHub(CancellationToken cancellationToken)
        {
            int max = 100;
            Random rnd = new();

            for (int i = 1; i <= max; i++)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                double temp = ((double)rnd.Next(-100, 300)) / 10;
                double hum = rnd.NextDouble() * 100;
                Data data = new(temp, hum);
                Console.WriteLine($"{i+1}/{max} Sending Data {{Temperature: {temp}, Humidity: {hum}}}");

                byte[] jsonData = JsonSerializer.SerializeToUtf8Bytes(data);
                await ioTHubModuleClient.SendEventAsync(new Message(jsonData), cancellationToken);
                Console.WriteLine($"{i+1}/{max} Sent Data {{Temperature: {temp}, Humidity: {hum}}}");

                if (i < max)
                    await Task.Delay(5000, cancellationToken);
            }
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };

            // Open a connection to the Edge runtime
            ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient);
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            var moduleClient = userContext as ModuleClient;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);
            Console.WriteLine($"Received message: {counterValue}, Body: [{messageString}]");

            if (!string.IsNullOrEmpty(messageString))
            {
                using (var pipeMessage = new Message(messageBytes))
                {
                    foreach (var prop in message.Properties)
                    {
                        pipeMessage.Properties.Add(prop.Key, prop.Value);
                    }
                    await moduleClient.SendEventAsync("output1", pipeMessage);

                    Console.WriteLine("Received message sent");
                }
            }
            return MessageResponse.Completed;
        }
    }
}
