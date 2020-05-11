package com.blob.ops;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.Properties;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

public class AzureBlobOperations {

	static File createTempFile() throws IOException {

		// creating a temporary file
		File tempFile = File.createTempFile("tempFile", ".txt");
		System.out.println("Creating a sample file at: " + tempFile.toString());
		Writer output = new BufferedWriter(new FileWriter(tempFile));
		output.write("Hello Azure blob Storage.");
		output.close();

		return tempFile;
	}

	public static void main(String[] args) throws IOException {

		final String resourceName = "storage.properties";
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
			props.load(resourceStream);
		}

		// Retrieve the credentials and initialize SharedKeyCredentials
		String accountName = props.getProperty("AZURE_STORAGE_ACCOUNT");
		String accountKey = props.getProperty("AZURE_STORAGE_ACCESS_KEY");
		String endpoint = "https://" + accountName + ".blob.core.windows.net";
		String containerName = props.getProperty("CONTAINER_NAME");

		// Create a SharedKeyCredential
		StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);

		// Create a blobServiceClient
		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential)
				.buildClient();

		// Create a containerClient
		BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);

		// Create a container
		blobServiceClient.createBlobContainer(containerName);
		System.out.printf("Creating a container : %s %n", blobContainerClient.getBlobContainerUrl());

		String blobName = props.getProperty("BLOB_NAME");
		// Create a BlobClient to run operations on Blobs
		BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

		// Listening for commands from the console
		System.out.println("Enter a command");

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				System.out.println(
						"\n (U)Upload Blob | (L)List Blobs | (G)Get Blob | (D)Delete Blobs | (P)Print BlobFile | (E)Exit");
				System.out.println("# Enter a command : ");
				String input = reader.readLine();

				switch (input.toUpperCase()) {

				// Upload a blob from a File
				case "U":
					System.out.println("Uploading the sample file into the container from a file: "
							+ blobContainerClient.getBlobContainerUrl());
					File tempFile = createTempFile();
					blobClient.uploadFromFile(tempFile.toPath().toString());
					tempFile.deleteOnExit();
					break;

				// List Blobs
				case "L":
					System.out.println("Listing blobs in the container: " + blobContainerClient.getBlobContainerUrl());
					blobContainerClient.listBlobs()
							.forEach(blobItem -> System.out.println("This is the blob name: " + blobItem.getName()));
					break;

				// Download a blob to local path
				case "G":
					System.out.println("Get(Download) the blob: " + blobClient.getBlobUrl());
					try (FileOutputStream fOutStream = new FileOutputStream(blobClient.getBlobName())) {
						blobClient.download(fOutStream);
						fOutStream.close();
					}
					break;

				// Delete a blob
				case "D":
					System.out.println("Delete the blob: " + blobClient.getBlobUrl());
					blobClient.delete();
					System.out.println();
					break;

				// Print Blob File contents
				case "P":
					String fileName = blobClient.getBlobName();
					System.out.println("Get(Download) the blob: " + blobClient.getBlobUrl());
					blobClient.download(new FileOutputStream(fileName));

					try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
						String line = null;
						while ((line = br.readLine()) != null) {
							System.out.println(line);
						}
						br.close();
					}
					break;

				// Exit
				case "E":
					System.out.println("Cleaning up the downloaded files and exiting.");
					File f = new File(blobClient.getBlobName());
					if (f.exists()) {
						f.delete();
					}
					blobContainerClient.delete();
					System.exit(0);
					break;

				default:
					break;
				}
			}
		}
	}
}
