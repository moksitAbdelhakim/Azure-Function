// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Extensions.Logging;
using Azure.Identity;
using Azure.DigitalTwins.Core;
using Azure.Core.Pipeline;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Azure;

namespace ExoSmart.Function
{
    public static class IoTHubtoDigitaleTwins
    {
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("IoTHubtoDigitaleTwins")]
        // Define an Azure function that trigger when an event is sent to an Event Grid topic
        public static async Task RunAsync([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            // Check if the Digital Twins service URL is set in the app settings, 
            // Add In the Function App Config, use: ADT_SERVICE_URL and set it to the URL for ADT diserd.
            if (adtInstanceUrl == null) log.LogError("Application setting \"ADT_SERVICE_URL\" not set");

            try
            {
                // Authenticate with Digital Twins using managed identity
                var cred = new ManagedIdentityCredential("https://digitaltwins.azure.net");
                // Create a client to call the Digital Twins API
                var client = new DigitalTwinsClient(
                    new Uri(adtInstanceUrl), cred,
                    new DigitalTwinsClientOptions
                    {
                        Transport = new HttpClientTransport(httpClient)
                    });

                log.LogInformation($"ADT service client connection created.");
                // Check if Event Grid event has data
                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    log.LogInformation(eventGridEvent.Data.ToString());
                    // Deserialize the event data into an array of JSON objects
                    JArray deviceMessages = (JArray)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());

                    foreach (JObject deviceMessage in deviceMessages)
                    {
                        // Extract device name, resource name and value from JSON object
                        string deviceName = (string)deviceMessage["deviceName"];
                        string resourceName = (string)deviceMessage["readings"][0]["resourceName"];
                        var value = deviceMessage["readings"][0]["value"];

                        switch (resourceName)
                        {
                            case "CO2":
                                log.LogInformation($"Device name: {deviceName} Resource name: {resourceName} CO2: {value}");
                                break;
                            case "CH4":
                                log.LogInformation($"Device name: {deviceName} Resource name: {resourceName} CH4: {value}");
                                break;
                            case "NH3":
                                log.LogInformation($"Device name: {deviceName} Resource name: {resourceName} NH3: {value}");
                                break;
                            case "N2O":
                                log.LogInformation($"Device name: {deviceName} Resource name: {resourceName} N2O: {value}");
                                break;
                        }
                        // Create a JSON Patch document to update the properties of the digital twin corresponding to the device
                        var updateTwinData = new JsonPatchDocument();
                        updateTwinData.AppendReplace($"/deviceName", deviceName);
                        updateTwinData.AppendReplace($"/{resourceName}", value.Value<float>());
                        // Call the asynchronous method to update the digital twin using the Digital Twins client
                        await client.UpdateDigitalTwinAsync(deviceName, updateTwinData);

                    }
                }
            }
            catch (Exception ex)
            {
                log.LogError($"Error in ingest function: {ex.Message}");
            }
        }
    }
}