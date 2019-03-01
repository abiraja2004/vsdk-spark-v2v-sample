package com.veeva.vault.custom.triggers;

import com.veeva.vault.sdk.api.data.RecordTriggerInfo;
import com.veeva.vault.sdk.api.job.JobParameters;
import com.veeva.vault.sdk.api.job.JobService;
import com.veeva.vault.sdk.api.query.QueryResponse;
import com.veeva.vault.sdk.api.query.QueryService;
import com.veeva.vault.sdk.api.data.RecordEvent;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.data.RecordTrigger;
import com.veeva.vault.sdk.api.data.RecordTriggerContext;
import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordChange;

import java.util.List;
import java.util.Map;

import com.veeva.vault.sdk.api.core.LogService;
import com.veeva.vault.sdk.api.core.RollbackException;
import com.veeva.vault.sdk.api.core.ServiceLocator;
import com.veeva.vault.sdk.api.core.ValueType;
import com.veeva.vault.sdk.api.core.VaultCollections;
import com.veeva.vault.sdk.api.core.VaultCollectors;
import com.veeva.vault.sdk.api.queue.*;

/**
 * This is a Vault Java SDK trigger that demonstrates a Spark vault to vault messaging queue. 
 * 
 * On a source vault, the trigger parses through inserted, updated, and deleted records:
 *    - Determine if the new vsdk_bike_store__c record is targeted for a Vault to Vault destination.
 *        - Set with a `purchase_order__c` value in `object_type__v` field.
 *    - Determine if the record is an insert, update, or delete. The `state` defines how the target handles the record.
 *        - If an insert:
 *            - `state` = "pending"
 *            - `record` = getNew()
 *            - `status` = true
 *        - If an update:
 *            - `state` = "approved"
 *            - `record` = getNew()
 *            - `status` = true if the old state was `pending` and the new state is `approved`
 *        - If an delete:
 *            - `state` = "deleted"
 *            - `record` = getOld()
 *            - `status` = true
 *    - Parse through all new inserted records and add their ID as items to the `Message`.
 *        - Set the 'state' attribute of the queue message to 'approved', 'pending', or 'deleted'.
 *        - Set the 'object' attribute of the queue message to 'vsdk_bike_store__c'.
 *        - Set the 'type' attribute of the queue message to 'purchase_order__c'.
 *    - Add the `Message` to the outbound queue - `vsdk_v2v_out_queue__c`.
 *    - Once added, mark the 'integration_status__c' field as 'message_queued__c' to single that the queuing was successful.
 */

@RecordTriggerInfo(object = "vsdk_bike_store__c", events = {RecordEvent.BEFORE_UPDATE, RecordEvent.AFTER_UPDATE, RecordEvent.BEFORE_INSERT, RecordEvent.AFTER_INSERT, RecordEvent.AFTER_DELETE})
public class vSDKVaultToVaultTriggerAll implements RecordTrigger {

    public void execute(RecordTriggerContext recordTriggerContext) {
    	
    	LogService logService = ServiceLocator.locate(LogService.class);
    	QueryService queryService = ServiceLocator.locate(QueryService.class);
        RecordEvent recordEvent = recordTriggerContext.getRecordEvent();
        
    	Map<String, Record> messageMap = VaultCollections.newMap();
    	Map<String,String> objectTypeMap = VaultCollections.newMap();
    	
    	String query = "SELECT id, name__v, api_name__v from object_type__v where object_name__v = 'vsdk_bike_store__c'";
    	QueryResponse queryResponse = queryService.query(query);
    	
    	queryResponse.streamResults().forEach(qr -> {
    		objectTypeMap.put(qr.getValue("id", ValueType.STRING),qr.getValue("api_name__v", ValueType.STRING));
            
        });
    	
    	String state = "";
    	for (RecordChange recordChange : recordTriggerContext.getRecordChanges()) {
    		boolean status = false;
    		Record record = null;
	    	if (recordEvent.toString().contains("INSERT")) {
	    		status = true;
	    		state = "pending";
	    		record = recordChange.getNew();
	    	}
	    	if (recordEvent.toString().contains("UPDATE")) {
	    		status = recordChange.getNew().getValue("state__v", ValueType.STRING).contains("approved") &&
				        recordChange.getOld().getValue("state__v", ValueType.STRING).contains("pending");
	    		state = "approved";
	    		record = recordChange.getNew();
	    	}
	    	if (recordEvent.toString().contains("DELETE")) {
	    		status = true;
	    		state = "deleted";
	    		record = recordChange.getOld();
	    	}

	    	//Before on operation on the record, check if it is a "purchase_order__c" type and then set some default field values.
	    	if (recordEvent.toString().equals("BEFORE_UPDATE") || recordEvent.toString().equals("BEFORE_INSERT")) {
	        	
	    		String typeId = record.getValue("object_type__v", ValueType.STRING);
	    		if (objectTypeMap.get(typeId).contains("purchase_order__c") && typeId != null && status == true) {
		    		record.setValue("source_record__c", "origin");
		    		record.setValue("source_vault__c", "origin");
	        	}
	    	}
	    	
	    	//After an operation on the record, check if it is a "purchase_order__c" type and then batch (up to 500 per batch) the inserted IDs.
	    	//Once the records are batched, execute the moveToQueue method to send the batch messageMap for processing into the Spark message queue.
	    	if (recordEvent.toString().contains("AFTER")) {
	    		String typeId = record.getValue("object_type__v", ValueType.STRING);
	    		if (objectTypeMap.get(typeId).contains("purchase_order__c") && typeId != null && status == true) {
		        	String id = record.getValue("id", ValueType.STRING);
		        	if (messageMap.size() < 500) {
		        		messageMap.put(id, record);
		        		logService.info("Added ID {} to Map.", id, state);
		        	}
		        	else {
		        		moveToQueue(messageMap, state);
		        		messageMap.clear();
		        	}
	        	}
	    	}	
    	}
    	
    	if (messageMap.size() > 0) {
    		moveToQueue(messageMap, state);
    	}  
    }

    public static void moveToQueue(Map<String,Record> messageMap, String state) {
    	
    	LogService logService = ServiceLocator.locate(LogService.class);
    	RecordService recordService = ServiceLocator.locate(RecordService.class);
    	List<Record> recordList = VaultCollections.newList();
    	
        //Get an instance of Job for invoking user actions, such as changing state and starting workflow
        JobService jobService = ServiceLocator.locate(JobService.class);
        JobParameters jobParameters = jobService.newJobParameters("record_user_action__v");
    	
    	//Spark Message Queue
        //Create a new Spark Message that is destined for the "vsdk_v2v_out_queue__c" queue.
        //Set message attributes and items and put message in a queue for delivery
    	QueueService queueService = ServiceLocator.locate (QueueService.class);
    	String queueName = "vsdk_v2v_out_queue__c";
    	Message message = queueService.newMessage(queueName)
        	.setAttribute ("object", "vsdk_bike_store__c")
        	.setAttribute ("type", "purchase_order__c")
        	.setAttribute ("state", state)
        	.setMessageItems(messageMap.keySet().stream().collect(VaultCollectors.toList()));
    	
    	//Put the new message into the Spark outbound queue. 
    	//The PutMessageResponse can be used to review if queuing was successful or not
    	PutMessageResponse response = queueService.putMessage(message);
    	
    	logService.info("Put 'vsdk_bike_store__c' Message in Queue - state = {}", state);
    	
    	//Check that the message queue successfully processed the message.
    	//If there is an error, create a record update to change the `integration_status__c` flag to 'queuing_failed__c'.
    	//If it's successful, create a record update to change the `integration_status__c` flag to 'message_queued__c'.
    	if (response.getError() != null) {
    		logService.info("ERROR Queuing Failed: " + response.getError().getMessage());
    		
    		if (!state.equals("deleted")) {
    			throw new RollbackException("SDK ERROR:", "Could not queue.");
    		}	
    		else {
                messageMap.values().stream().forEach(record -> {
                	Record recordUpdate = recordService.newRecordWithId("vsdk_bike_store__c", record.getValue("id", ValueType.STRING));
                	recordUpdate.setValue("integration_status__c", VaultCollections.asList("queuing_failed__c"));
                	recordList.add(recordUpdate);
                }); 	
    		}
    	}
    	else {
    		response.getPutMessageResults().stream().forEach( result -> {
        		logService.info("SUCCESS: " + result.getMessageId() + " " + result.getConnectionName());
        		
                //For inserted record, start a workflow for each record by invoking start_vsdk_bs_workflow_useraction__c user action.
        		if(state.equals("pending")) {
	                jobParameters.setValue("user_action_name", "start_vsdk_bs_workflow_useraction__c");
	                jobParameters.setValue("records", messageMap.values().stream().collect(VaultCollectors.toList()));
	                jobService.run(jobParameters);
        		}
        		
    		    //Create a record update to change the `integration_status__c` flag to 'message_queued__c'.
        		if (!state.equals("deleted")) {
	                messageMap.values().stream().forEach(record -> {
	                	Record recordUpdate = recordService.newRecordWithId("vsdk_bike_store__c", record.getValue("id", ValueType.STRING));
	                	recordUpdate.setValue("integration_status__c", VaultCollections.asList("message_queued__c"));
	                	recordList.add(recordUpdate);
	                });
        		}	
        	});
    	}
    	
    	if (recordList.size() > 0) {
	    	 recordService.batchSaveRecords(recordList)
	         .onErrors(batchOperationErrors -> {
	           //Iterate over the caught errors. 
	           //The BatchOperation.onErrors() returns a list of BatchOperationErrors. 
	           //The list can then be traversed to retrieve a single BatchOperationError and 
	           //then extract an **ErrorResult** with BatchOperationError.getError(). 
	   	      batchOperationErrors.stream().findFirst().ifPresent(error -> {
	 	          String errMsg = error.getError().getMessage();
	 	          int errPosition = error.getInputPosition();
	 	          String name = recordList.get(errPosition).getValue("name__v", ValueType.STRING);
	 	          throw new RollbackException("OPERATION_NOT_ALLOWED", "Unable to create '" + recordList.get(errPosition).getObjectName() + "' record: '" +
	 	                  name + "' because of '" + errMsg + "'.");
	 	      });
	         })
	         .execute();
    	}
    }
}