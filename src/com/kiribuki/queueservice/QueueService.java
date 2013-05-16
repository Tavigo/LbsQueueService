package com.kiribuki.queueservice;
	/*
	 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
	 *
	 * Licensed under the Apache License, Version 2.0 (the "License").
	 * You may not use this file except in compliance with the License.
	 * A copy of the License is located at
	 *
	 *  http://aws.amazon.com/apache2.0
	 *
	 * or in the "license" file accompanying this file. This file is distributed
	 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
	 * express or implied. See the License for the specific language governing
	 * permissions and limitations under the License.
	 */
import java.util.List;
//import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
//import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

//import com.amazonaws.services.sqs.model.GetQueueUrlRequest;


public class QueueService {
	
	private boolean swok = false;
	private AmazonSQS sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());
	private String myQueueUrl= "";
	
	/*
	 * Constructor de la clase: nos abrirá una connexión con el servicio de colas de Amazom.
	 * Si hay alguna excepción cargaremos la variable swok a false, para no poer operar con el objeto
	 */
	public QueueService() throws Exception {
		Region euWest1 = Region.getRegion(Regions.EU_WEST_1);
		try {
			sqs.setRegion(euWest1);
			swok = true;
		} catch (Exception e) {
			swok = false;
		}
	}
	
	/*
	 *  Función que utilizaremos para crear una cola en el servicio de colas de Amazon.
	 *  Como parámetro necesitamos el nombre de la cola a crear en el servicio de colas de Amazon
	 */
	public boolean CreateQueue (String QueueName) {
		if (swok == true) {
			try {
				CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueName);
				myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
				return true;
			} catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which means the client encountered " +
						"a serious internal problem while trying to communicate with SQS, such as not " +
						"being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
				swok = false;
			}
		} else {
			System.out.println("NO ESTA OK L'OBJECTE");
			swok = false;
		}
		return swok;
	}
	
	
	/*
	 * Función para enviar un mensage a la cola
	 */
	public boolean SendMessage(String message) {
		if (swok == true) {
			try {
				sqs.sendMessage(new SendMessageRequest(myQueueUrl, message));
				swok = true;
			}  catch (AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which means your request made it " +
						"to Amazon SQS, but was rejected with an error response for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				swok=false;
			}
		}
		return swok;
	}

	//Función que recibe mensajes de la cola
	private Message ReceiveMessage() {
		// Recepció de mensajes
		Message newmessage=null;
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
    		
		for (Message message : messages) {
			//System.out.println("  Message");
			//System.out.println("    MessageId:     " + message.getMessageId());
			//System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
			//System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
			//System.out.println("    Body:          " + message.getBody());
			newmessage = message;
		}
		
		return newmessage;
	}
	
	
	
	private void DeleteMessage( String MessageHandle) {  
        sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, MessageHandle));
	}
	
	
	public String ReceiveDeleteMessage () {
		Message newmessage = ReceiveMessage();
		DeleteMessage(newmessage.getReceiptHandle());
		return newmessage.getBody();
	}
	
	/*
	 * Función para leer si el Objeto esta Ok o no
	 */
	public boolean QueueServiceOk() {
		return swok;
	}
}

