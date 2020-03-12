using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Net.Http.Headers;

namespace omnigage_imports
{
    public class Program
    {
        static void Main(string[] args)
        {
            // Set token retrieved from Account -> Developer -> API Tokens
            var tokenKey = "";
            var tokenSecret = "";

            // Retrieve from Account -> Settings -> General -> "Key" field
            var accountKey = "";

            // Local path to a XLSX or CSV
            // On Mac, for example: "/Users/Shared/import-example.xlsx"
            var filePath = "";

            // API host path, only change if using sandbox
            var host = "https://api.omnigage.io/api/v1/";

            Task.Run(() => MainAsync(tokenKey, tokenSecret, accountKey, filePath, host));
            Console.ReadLine();
        }

        static async Task MainAsync(string tokenKey, string tokenSecret, string accountKey, string filePath, string host)
        {
            // Check that the file exists
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"File {filePath} not found.");
            }

            // Build basic authorization
            var authBytes = System.Text.Encoding.UTF8.GetBytes($"{tokenKey}:{tokenSecret}");
            var authEncoded = System.Convert.ToBase64String(authBytes);

            // Collect meta on the file
            var fileName = Path.GetFileName(filePath);
            long fileSize = new System.IO.FileInfo(filePath).Length;
            string extension = Path.GetExtension(fileName);
            string mimeType = null;

            // Determine MIME type
            if (extension == ".xlsx")
            {
                mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            }
            else if (extension == ".csv")
            {
                mimeType = "text/csv";
            }
            else
            {
                throw new System.InvalidOperationException("Only CSV or XLSX files accepted.");
            }

            Console.WriteLine($"File name: {fileName}");
            Console.WriteLine($"File type: {mimeType}");
            Console.WriteLine($"File size: {fileSize}");

            using (var client = new HttpClient())
            {
                // Set request context for Omnigage API
                client.BaseAddress = new Uri(host);
                client.DefaultRequestHeaders.Add("Authorization", "Basic " + authEncoded);
                client.DefaultRequestHeaders.Add("X-Account-Key", accountKey);

                // Build `upload` instance payload
                string uploadContent = @"{
                   'name': '" + fileName + @"',
                   'type': '" + mimeType + @"',
                   'size': "  + fileSize + @"
                }";

                var uploadPayload = new StringContent(uploadContent, Encoding.UTF8, "application/json");

                var uploadRequest = await client.PostAsync("uploads", uploadPayload);
                string uploadResponse = await uploadRequest.Content.ReadAsStringAsync();
                JObject uploadInstance = JObject.Parse(uploadResponse);

                // Retrieve values to use for uploading to S3
                string requestUrl = (string) uploadInstance.SelectToken("data.attributes.request-url");
                string uploadId = (string)uploadInstance.SelectToken("data.id");
                object[] requestHeaders = uploadInstance.SelectToken("data.attributes.request-headers").Select(s => (object)s).ToArray();
                object[] requestFormData = uploadInstance.SelectToken("data.attributes.request-form-data").Select(s => (object)s).ToArray();

                Console.WriteLine($"Upload ID: {uploadId}");

                using (var clientS3 = new HttpClient())
                {
                    // Set each of the `upload` instance headers
                    foreach (JObject header in requestHeaders)
                    {
                        foreach (KeyValuePair<string, JToken> prop in header)
                        {
                            clientS3.DefaultRequestHeaders.Add(prop.Key, (string) prop.Value);
                        }
                    }

                    var form = new MultipartFormDataContent("Upload----" + DateTime.Now.ToString(CultureInfo.InvariantCulture));

                    // Set each of the `upload` instance form data
                    foreach (JObject formData in requestFormData)
                    {
                        foreach (KeyValuePair<string, JToken> prop in formData)
                        {
                            form.Add(new StringContent((string) prop.Value), prop.Key);
                        }
                    }

                    // Set the content type (required by presigned URL)
                    form.Add(new StringContent(mimeType), "Content-Type");

                    // Add file content to form
                    using var fileContent = new ByteArrayContent(await File.ReadAllBytesAsync(filePath));
                    fileContent.Headers.ContentType = MediaTypeHeaderValue.Parse("multipart/form-data");
                    form.Add(fileContent, "file", fileName);

                    // Make S3 request
                    var responseS3 = await clientS3.PostAsync(requestUrl, form);
                    string responseContent = await responseS3.Content.ReadAsStringAsync();
                    if ((int)responseS3.StatusCode == 204)
                    {
                        Console.WriteLine("Successfully uploaded file.");
                    }
                    else
                    {
                        Console.WriteLine(responseS3);
                    }
                }

                string importContent = @"{
                   ""data"":{
                      ""attributes"":{
                         ""status"":""queued"",
                         ""unique-primary-phone"":true
                      },
                      ""relationships"":{
                         ""upload"":{
                            ""data"":{
                               ""type"":""uploads"",
                               ""id"":""" + uploadId + @"""
                            }
                         }
                      },
                      ""type"":""import-contacts""
                   }
                }";

                var importPayload = new StringContent(importContent, Encoding.UTF8, "application/json");
                var importRequest = await client.PostAsync("import-contacts", importPayload);
                string importResponse = await importRequest.Content.ReadAsStringAsync();
                JObject importInstance = JObject.Parse(importResponse);
                string importId = (string)importInstance.SelectToken("data.id");
                Console.WriteLine($"Import ID: {importId}");
            }
        }
    }
}
