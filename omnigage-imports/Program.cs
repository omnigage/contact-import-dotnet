using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;


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

            try
            {
                MainAsync(tokenKey, tokenSecret, accountKey, filePath, host).Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        static async Task MainAsync(string tokenKey, string tokenSecret, string accountKey, string filePath, string host)
        {
            // Check that the file exists
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"File {filePath} not found.");
            }

            // Collect meta on the file
            var fileName = Path.GetFileName(filePath);
            long fileSize = new System.IO.FileInfo(filePath).Length;
            string mimeType = getMimeType(fileName);

            // Ensure proper MIME type
            if (mimeType == null)
            {
                throw new System.InvalidOperationException("Only CSV or XLSX files accepted.");
            }

            Console.WriteLine($"File: {fileName}");

            using (var client = new HttpClient())
            {
                // Build basic authorization
                var authorization = createAuthorization(tokenKey, tokenSecret);

                // Set request context for Omnigage API
                client.BaseAddress = new Uri(host);
                client.DefaultRequestHeaders.Add("Authorization", "Basic " + authorization);
                client.DefaultRequestHeaders.Add("X-Account-Key", accountKey);

                // Build `upload` instance payload and make request
                string uploadContent = createUploadSchema(fileName, mimeType, fileSize);
                JObject uploadResponse = await postUploadRequest(client, uploadContent);

                // Extract upload ID and request URL
                string uploadId = (string)uploadResponse.SelectToken("data.id");
                string requestUrl = (string)uploadResponse.SelectToken("data.attributes.request-url");

                Console.WriteLine($"Upload ID: {uploadId}");

                using (var clientS3 = new HttpClient())
                {
                    // Create multipart form including setting form data and file content
                    MultipartFormDataContent form = await createMultipartForm(uploadResponse, filePath, fileName, mimeType);

                    // Upload to S3
                    await postS3Request(clientS3, uploadResponse, form, requestUrl);

                    // Create import
                    string importContent = createImportContactSchema(uploadId);
                    JObject importResponse = await postImportContactRequest(client, importContent);

                    // Extract import id
                    string importId = (string)importResponse.SelectToken("data.id");

                    Console.WriteLine($"Import ID: {importId}");
                };
            };
        }

        static async Task<JObject> postUploadRequest(HttpClient client, string content)
        {
            var uploadPayload = new StringContent(content, Encoding.UTF8, "application/json");
            var uploadRequest = await client.PostAsync("uploads", uploadPayload);
            string uploadResponse = await uploadRequest.Content.ReadAsStringAsync();
            return JObject.Parse(uploadResponse);
        }

        static async Task postS3Request(HttpClient client, JObject uploadInstance, MultipartFormDataContent form, string url)
        {
            object[] requestHeaders = uploadInstance.SelectToken("data.attributes.request-headers").Select(s => (object)s).ToArray();

            // Set each of the `upload` instance headers
            foreach (JObject header in requestHeaders)
            {
                foreach (KeyValuePair<string, JToken> prop in header)
                {
                    client.DefaultRequestHeaders.Add(prop.Key, (string)prop.Value);
                }
            }

            // Make S3 request
            var responseS3 = await client.PostAsync(url, form);
            string responseContent = await responseS3.Content.ReadAsStringAsync();

            if ((int)responseS3.StatusCode == 204)
            {
                Console.WriteLine("Successfully uploaded file.");
            }
            else
            {
                Console.WriteLine(responseS3);
                throw new S3UploadFailed();
            }
        }

        static async Task<JObject> postImportContactRequest(HttpClient client, string content)
        {
            var importPayload = new StringContent(content, Encoding.UTF8, "application/json");
            var importRequest = await client.PostAsync("import-contacts", importPayload);
            string importResponse = await importRequest.Content.ReadAsStringAsync();
            return JObject.Parse(importResponse);
        }

        static async Task<MultipartFormDataContent> createMultipartForm(JObject uploadInstance, string filePath, string fileName, string mimeType)
        {
            // Retrieve values to use for uploading to S3
            object[] requestFormData = uploadInstance.SelectToken("data.attributes.request-form-data").Select(s => (object)s).ToArray();

            var form = new MultipartFormDataContent("Upload----" + DateTime.Now.ToString(CultureInfo.InvariantCulture));

            // Set each of the `upload` instance form data
            foreach (JObject formData in requestFormData)
            {
                foreach (KeyValuePair<string, JToken> prop in formData)
                {
                    form.Add(new StringContent((string)prop.Value), prop.Key);
                }
            }

            // Set the content type (required by presigned URL)
            form.Add(new StringContent(mimeType), "Content-Type");

            // Add file content to form
            var fileContent = new ByteArrayContent(await File.ReadAllBytesAsync(filePath));
            fileContent.Headers.ContentType = MediaTypeHeaderValue.Parse("multipart/form-data");
            form.Add(fileContent, "file", fileName);

            return form;
        }

        static string getMimeType(string fileName)
        {
            string extension = Path.GetExtension(fileName);

            if (extension == ".xlsx")
            {
                return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            }
            else if (extension == ".csv")
            {
                return "text/csv";
            }

            return null;
        }

        static string createUploadSchema(string fileName, string mimeType, long fileSize)
        {
            return @"{
                'name': '" + fileName + @"',
                'type': '" + mimeType + @"',
                'size': " + fileSize + @"
            }";
        }

        static string createImportContactSchema(string uploadId)
        {
            return @"{
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
        }

        static string createAuthorization(string key, string secret)
        {
            var authBytes = System.Text.Encoding.UTF8.GetBytes($"{key}:{secret}");
            return System.Convert.ToBase64String(authBytes);
        }

        public class S3UploadFailed : System.Exception {}
    }
}
