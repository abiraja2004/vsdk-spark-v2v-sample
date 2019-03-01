package com.veeva.vault.custom.actions;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.action.DocumentAction;
import com.veeva.vault.sdk.api.action.DocumentActionContext;
import com.veeva.vault.sdk.api.action.DocumentActionInfo;
import com.veeva.vault.sdk.api.action.Usage;
import com.veeva.vault.sdk.api.document.*;
import com.veeva.vault.sdk.api.job.JobParameters;
import com.veeva.vault.sdk.api.job.JobService;
import com.veeva.vault.sdk.api.query.QueryResponse;
import com.veeva.vault.sdk.api.query.QueryResult;
import com.veeva.vault.sdk.api.query.QueryService;
import com.veeva.vault.sdk.api.queue.Message;
import com.veeva.vault.sdk.api.queue.PutMessageResponse;
import com.veeva.vault.sdk.api.queue.QueueService;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class demonstrates how to use a Document User Action to send a document's information to a Spark Messaging Queue.
 * For this use case, the "vSDK: Create Vault to Vault Crosslink" user action is run on a "vSDK Document". 
 * The user action will perform the following:
 * 
 * 		- Iterate over the documents and create one Spark Message per document
 *      - The message will get added to the "vsdk_v2v_out_queue" outbound queue
 *      - The message should contain all the required fields for creating a crosslink in the target vault
 * 
 */

@DocumentActionInfo(label="vSDK: Create Vault to Vault Crosslink")
public class vSDKCrosslinkDocumentAction implements DocumentAction {
	
    public void execute(DocumentActionContext documentActionContext) {

    	DocumentService documentService = ServiceLocator.locate((DocumentService.class));
    	LogService logService = ServiceLocator.locate(LogService.class);
    	
    	List<DocumentVersion> documentList = VaultCollections.newList();
    	
    	for (DocumentVersion documentVersion : documentActionContext.getDocumentVersions()) {
        	String docId = documentVersion.getValue("id", ValueType.STRING);
        	String name = documentVersion.getValue("name__v", ValueType.STRING);
        	        	
        	//Spark Message Queue
            //Create a new Spark Message that is destined for the "vsdk_v2v_out_queue__c" queue.
            //Set message attributes and items and put message in a queue for delivery
        	QueueService queueService = ServiceLocator.locate (QueueService.class);
        	String queueName = "vsdk_v2v_out_queue__c";
        	Message message = queueService.newMessage(queueName)
            	.setAttribute ("object", "documents")
            	.setAttribute ("type", "vSDK Document")
            	.setAttribute ("state", "crosslink")
            	.setAttribute ("docId", docId)
            	.setAttribute ("docName", name);
        	
        	//Put the new message into the Spark outbound queue. 
        	//The PutMessageResponse can be used to review if queuing was successful or not
        	PutMessageResponse response = queueService.putMessage(message);
        	
        	logService.info("Put 'document' Message in Queue - state = 'crosslink");
        	
        	//Check that the message queue successfully processed the message.
        	//If there is an error, create a document update to change the `integration_status__c` flag to 'queuing_failed__c'.
        	//If it's successful, create a document update to change the `integration_status__c` flag to 'message_queued__c'.
        	if (response.getError() != null) {
        		logService.info("ERROR Queuing Failed: " + response.getError().getMessage());

            	DocumentVersion documentUpdate = documentService.newDocumentWithId(documentVersion.getValue("id", ValueType.STRING));
            	documentUpdate.setValue("vsdk_integration_status__c", VaultCollections.asList("queuing_failed__c"));
            	documentList.add(documentUpdate);	
        	}
        	else {
        		response.getPutMessageResults().stream().forEach( result -> {
            		logService.info("SUCCESS: " + result.getMessageId() + " " + result.getConnectionName());
            		
        		    //Create a record update to change the `integration_status__c` flag to 'message_queued__c'.
                	DocumentVersion documentUpdate = documentService.newDocumentWithId(documentVersion.getValue("id", ValueType.STRING));
                	documentUpdate.setValue("vsdk_integration_status__c", VaultCollections.asList("message_queued__c"));
                	documentList.add(documentUpdate);
            	});
        	}
    	}
    	
    	if (documentList.size() > 0) {
    		documentService.saveDocumentVersions(documentList);
    	}
    }

	public boolean isExecutable(DocumentActionContext documentActionContext) {
	    return true;
	}
}