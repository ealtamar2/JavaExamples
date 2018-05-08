package com.example.aws.s3.communication;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.util.Base64;

/**
 * S3 Request Maneja los datos de conexion y las operaciones CRUD con S3 de AWS

 * @version 1.0
 */
public class S3Request implements Serializable {

	private static final long serialVersionUID = 8455563583334998592L;
	private static final String HEADER_ENCRYPT_ID = "x-amz-server-side-encryption-aws-kms-key-id";
	private static final String HEADER_ENCRYPT_ALGORITHM = "x-amz-server-side-encryption";

	private static Map<String, S3Request> mapInstance = new TreeMap<>();
	private static Map<String, AmazonS3> mapClient = new TreeMap<>();
	private static Map<String, String> mapEndpoint = new TreeMap<>();
	private static Map<String, String> mapKeyEncrypt = new TreeMap<>();

	/**
	 * Obtiene la instancia del objeto
	 * 
	 * @param endpoint
	 * @param keyAccess
	 * @param keySecret
	 * @param keyEncrypt
	 * @return
	 */
	public static S3Request getInstance(String endpoint, String bucket,
			String keyAccess, String keySecret, String keyEncrypt) {
		S3Request instance = null;
		if (!mapInstance.containsKey(bucket)) {
			instance = new S3Request(endpoint, bucket, keyAccess, keySecret,
					keyEncrypt);
			mapInstance.put(bucket, instance);
		} else {
			instance = mapInstance.get(bucket);
		}
		return instance;
	}

	/**
	 * Crea una instancia de S3Request, adiciona los parametros relacionados al bucket
	 * @param endpoint
	 * @param bucket
	 * @param keyAccess
	 * @param keySecret
	 * @param keyEncrypt
	 */
	private S3Request(String endpoint, String bucket, String keyAccess,
			String keySecret, String keyEncrypt) {

		AWSCredentials credentials = new BasicAWSCredentials(keyAccess, keySecret);
		AmazonS3Client client = new AmazonS3Client(credentials);
		
		client.setEndpoint(endpoint);
		mapClient.put(bucket, client);
		mapEndpoint.put(bucket, endpoint);
		mapKeyEncrypt.put(bucket, keyEncrypt);
	}

	/**
	 * Realiza la operacion de Upload en S3
	 * @param bucket
	 * @param fileKey
	 * @param base64Data
	 * @param encrypt
	 * @return
	 */
	public String uploadImage(String bucket, String fileKey, String base64Data, boolean encrypt) {

		try {
			ObjectMetadata metadata = new ObjectMetadata();
			byte[] stream = base64Data.getBytes();
			stream = Base64.decode(stream);

			ByteArrayInputStream input = new ByteArrayInputStream(stream);
			metadata.setContentLength(stream.length);

			PutObjectResult result = null;
			AmazonS3 client = mapClient.get(bucket);
			if (client != null) {
				PutObjectRequest putRequest = new PutObjectRequest(bucket,
						fileKey, input, metadata);
				String keyEncriptID = mapKeyEncrypt.get(bucket);

				try {
					if (encrypt && !isEmpty(keyEncriptID)) {
						ObjectMetadata om = new ObjectMetadata();
						om.setHeader(HEADER_ENCRYPT_ALGORITHM, SSEAlgorithm.KMS.getAlgorithm());
						om.setHeader(HEADER_ENCRYPT_ID, keyEncriptID);
						om.setContentType("image/jpeg");
						om.setContentEncoding("base64");
						putRequest.setMetadata(om);
					}
					result = client.putObject(putRequest);
				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}
			}

			String url = null;
			if (result != null && !isEmpty(result.toString())) {
				String endpoint = mapEndpoint.get(bucket);
				url = String.format("http://%s/%s/%s", endpoint, bucket,
						fileKey);
			}
			return url;
		} catch (Exception e){
			e.printStackTrace();
			throw e;
		}
		
	}
	
	/**
	 * Generate URL with pre-signed data
	 * @param s3client
	 * @param bucket
	 * @param fileKey
	 * @param keyEncriptID
	 * @return
	 */
	public String generatePresignedURL(AmazonS3 s3client, String bucket,
			String fileKey, String keyEncriptID) {
		GeneratePresignedUrlRequest presignedUrlReq;
		presignedUrlReq = new GeneratePresignedUrlRequest(bucket, fileKey,
				HttpMethod.GET);

		java.util.Date expiration = new java.util.Date();
		long milliSeconds = expiration.getTime();
		milliSeconds += (48000 * 60 * 60); // Add 48 hour.
		expiration.setTime(milliSeconds);

		presignedUrlReq.setExpiration(expiration);
		presignedUrlReq.setContentType("image/jpeg");
		if (!isEmpty(keyEncriptID)) {
			presignedUrlReq.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
			presignedUrlReq.setKmsCmkId(keyEncriptID);
		}

		URL url = s3client.generatePresignedUrl(presignedUrlReq);
		return url.toString();
	}

	/**
	 * Elimima del repositorio S3 los elementos referenciados por las llaves
	 * entregadas
	 * 
	 * @param keys
	 * @return
	 */
	public DeleteObjectsResult deleteImage(String bucket, String... keys)
			throws AmazonClientException, AmazonServiceException {
		List<KeyVersion> listKeys = new ArrayList<>();
		for (String string : keys) {
			listKeys.add(new KeyVersion(string));
		}
		DeleteObjectsRequest deleteObjectsRequest = null;
		deleteObjectsRequest = new DeleteObjectsRequest(bucket);
		deleteObjectsRequest.setKeys(listKeys);

		DeleteObjectsResult delRes = null;
		AmazonS3 client = mapClient.get(bucket);
		if (client != null) {
			delRes = client.deleteObjects(deleteObjectsRequest);
		}
		return delRes;
	}

	/**
	 * Check string text is empty or not
	 * 
	 * @param text
	 * @return
	 */
	private static boolean isEmpty(String text) {
		boolean res = (text == null || text.isEmpty() || "".equals(text));
		return res;
	}

}
