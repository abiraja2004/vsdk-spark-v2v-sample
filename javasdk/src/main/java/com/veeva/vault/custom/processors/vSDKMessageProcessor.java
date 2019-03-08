package com.veeva.vault.custom.processors;

import com.veeva.vault.sdk.api.data.RecordTriggerInfo;
import com.veeva.vault.sdk.api.http.HttpMethod;
import com.veeva.vault.sdk.api.http.HttpRequest;
import com.veeva.vault.sdk.api.http.HttpResponseBodyValueType;
import com.veeva.vault.sdk.api.http.HttpService;
import com.veeva.vault.sdk.api.job.JobParameters;
import com.veeva.vault.sdk.api.job.JobService;
import com.veeva.vault.sdk.api.json.JsonArray;
import com.veeva.vault.sdk.api.json.JsonArrayBuilder;
import com.veeva.vault.sdk.api.json.JsonData;
import com.veeva.vault.sdk.api.json.JsonObject;
import com.veeva.vault.sdk.api.json.JsonObjectBuilder;
import com.veeva.vault.sdk.api.json.JsonService;
import com.veeva.vault.sdk.api.json.JsonValueType;
import com.veeva.vault.sdk.api.picklist.Picklist;
import com.veeva.vault.sdk.api.picklist.PicklistService;
import com.veeva.vault.sdk.api.query.QueryResponse;
import com.veeva.vault.sdk.api.query.QueryService;
import com.veeva.vault.sdk.api.data.RecordEvent;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.data.RecordTrigger;
import com.veeva.vault.sdk.api.data.RecordTriggerContext;
import com.veeva.vault.sdk.api.data.RecordChange;
import com.veeva.vault.sdk.api.data.Record;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import com.veeva.vault.sdk.api.core.LogService;
import com.veeva.vault.sdk.api.core.RollbackException;
import com.veeva.vault.sdk.api.core.ServiceLocator;
import com.veeva.vault.sdk.api.core.StringUtils;
import com.veeva.vault.sdk.api.core.ValueType;
import com.veeva.vault.sdk.api.core.VaultCollections;
import com.veeva.vault.sdk.api.core.VaultCollectors;
import com.veeva.vault.sdk.api.queue.*;

/**
 * 
 * This is a Vault Java SDK MessageProcessor that demonstrates a Spark vault to vault messaging queue. 
 * 
 * On the target vault, the MessageProcessor processes `Message` records received in the inbound queue - `vsdk_v2v_in_queue__c`.
 *   
 *	  - As a `Message` comes in, the `state` attribute determines if it is a new or existing record.
 *		  - For the `pending` state, a new vsdk_purchase_order__c record is created in the target vault.
 *		      - The `source_vault__c` field is set to the source vault's ID.
 *		      - The `source_record__c` field is set to the source record's ID.
 *		  - For the `approved` state, an existing vsdk_purchase_order__c record is updated.
 *            - Use the `HttpService` to run a query against the source vault for more data
 *            - Use the `HttpService` to a run an update against the source vault to update the `integration_status__c` flag.
 *        - For the `deleted` state, delete a vsdk_purchase_order__c record if it exists.
 *	  - Only non-empty `Message` records are inserted.
 * 
 */

@MessageProcessorInfo()
public class vSDKMessageProcessor implements MessageProcessor {
	
    public void execute(MessageContext context) {

    	//Message Processor logic
    	//This grabs a single message off the queue and makes it available for parsing.
    	//If it has the correct attributes, we forward to a simpleMessageType, callbackMessageType, or localCreateCrosslink method that processes the data.
    	
        Message message = context.getMessage(); 
        
        
        //Processes a message from the queue without making a callback to the source vault.
    	//Here we want to grab non-empty messages and make sure that the message has a `vsdk_bike_store__c` attribute.
        //This processing creates a new record and assigns default values like:
        //     Source record ID
        //     Source vault ID
        //     Integration status
        
        if (!message.getMessageItems().isEmpty() && 
        	message.getAttribute("object", MessageAttributeValueType.STRING).toString().contains("vsdk_bike_store__c") &&
        	message.getAttribute("state", MessageAttributeValueType.STRING).toString().contains("pending")) {
        	
        	simpleMessageType(message, context);
        }
        
        
        //Processes a message from the queue and makes a HTTP callback to the source vault.
    	//Here we want to grab non-empty messages and make sure that the message has a `vsdk_bike_store__c` attribute.
        //This processing searches for an existing record based on the incoming message which has the source record ID.
        //Once a record is found, an API VQL query is made to the source vault to grab data from the source record.
        //This queried information is added to the target record.
        //After it is updated, an API record update to sent to the source vault to update the `integration_status__c`:
        //     `successfully_processed__c` - if the query and update are successful
        //     `processing_failed__c`       - if the query or update are unsuccessful
        
        if (!message.getMessageItems().isEmpty() && 
            message.getAttribute("object", MessageAttributeValueType.STRING).toString().contains("vsdk_bike_store__c") &&
            (message.getAttribute("state", MessageAttributeValueType.STRING).toString().contains("approved") ||
             message.getAttribute("state", MessageAttributeValueType.STRING).toString().contains("deleted"))) {
            	
        	callbackMessageType(message, context);
        }
        
        
        //Processes a document message from the queue and creates a crosslink document.
        //This uses a local Vault API connection to create the document in the target vault.
        if (message.getAttribute("object", MessageAttributeValueType.STRING).toString().contains("documents") &&
            message.getAttribute("state", MessageAttributeValueType.STRING).toString().contains("crosslink")) {
            	
        	HttpCallouts.localCreateCrosslink(message, context);
        }
    }
    
    
    
    
//Processes a message from the queue without making a callback to the source vault.
//This processing creates a new record if it doesn't exist and assigns default values like:
//     Source record ID
//     Source vault ID
//     Integration status
    
    public static void simpleMessageType(Message message, MessageContext context) {
    	
    	LogService logService = ServiceLocator.locate(LogService.class);
    	RecordService recordService = ServiceLocator.locate(RecordService.class);
    	QueryService queryService = ServiceLocator.locate(QueryService.class);
    	
        //Get an instance of Job for invoking user actions, such as changing state and starting workflow
        JobService jobService = ServiceLocator.locate(JobService.class);
        JobParameters jobParameters = jobService.newJobParameters("record_user_action__v");
    	
    	Map<String,String> destinationObjectMap = VaultCollections.newMap();
    	destinationObjectMap.put("purchase_order__c", "vsdk_purchase_order__c");
    	
    	List<Record> processingRecordList = VaultCollections.newList();
    	
    	//Streams require final values - used later to set and check values.
    	final String messageState = message.getAttribute("state", MessageAttributeValueType.STRING).toString().toLowerCase();
    	final String destinationObject = destinationObjectMap.get(message.getAttribute("type", MessageAttributeValueType.STRING).toString().toLowerCase());
    	
    	//Message Logic
    	//Retrieve all items in the Message. This list of items can be up to 500 items per message.    	
    	//Create new `vsdk_purchase_order__c` records with the following fields defaulted:
		//      Source record ID
		//      Source vault ID
		//      Integration status    	
    	
    	Map<String,String> incomingMessageMap = VaultCollections.newMap();
    	
    	//Retrieve all items in the Message and put them in a Map. 
    	//The map is used to determine if the code wasn't able to find existing records.
    	for (String sourceRecordId : message.getMessageItems()) {
            incomingMessageMap.put(sourceRecordId, "true");
            logService.info("Incoming message item: " + sourceRecordId);
    	}
    
    	//Query to see any incoming IDs match to any existing vsdk_purchase_order__c records.
    	//This is necessary just in a case an `approved` message is processed before a new `pending` message.
	    String query = "select id, source_record__c from " + destinationObject 
	    		     + " where source_record__c contains ('" + String.join("','", incomingMessageMap.keySet())  + "')";
	    QueryResponse queryResponse = queryService.query(query);
	    
	    logService.info("Query: " + query);
	    
		queryResponse.streamResults().forEach(qr -> {
	        String id = qr.getValue("id", ValueType.STRING);
	        String source_id = qr.getValue("source_record__c", ValueType.STRING);
	        
	        logService.info("Found existing record with ID: " + id);

	        incomingMessageMap.remove(source_id);       
	    });
		queryResponse = null;
	    
    	
		//If there are `pending` message items that don't already exist in the target vault, then go ahead and create them.
    	//Create new `vsdk_purchase_order__c` records with the following fields defaulted:
		//      Source record ID
		//      Source vault ID
		//      Integration status
		if (messageState.equals("pending")){
			for (String key : incomingMessageMap.keySet()) {
		    	Record newSparkRecord = recordService.newRecord(destinationObject);
		    	
		    	newSparkRecord.setValue("source_vault__c", context.getRemoteVaultId().toString());
		    	newSparkRecord.setValue("source_record__c", key);
		    	newSparkRecord.setValue("integration_status__c", VaultCollections.asList("message_received__c"));
		    	
		    	processingRecordList.add(newSparkRecord);
			}
		}
    	
    	
		if (processingRecordList.size() > 0) {
			recordService.batchSaveRecords(processingRecordList).onSuccesses(successMessage -> {
		    	
		    	List<Record> successfulRecords = VaultCollections.newList();
		    	successMessage.stream().forEach(positionalRecordId -> {
		    		Record record = recordService.newRecordWithId(destinationObject, positionalRecordId.getRecordId());
		    		
		    		successfulRecords.add(record);
		    	});
		    	
		        //Use Job Service to change the status depending on the status of the incoming record. 
		        if (messageState.equals("pending")) {
		        	jobParameters.setValue("user_action_name", "change_state_to_pending_useraction__c");
		            jobParameters.setValue("records",successfulRecords);
		            jobService.run(jobParameters);
		        }
		   	}).onErrors(batchOperationErrors -> {
		   		batchOperationErrors.stream().findFirst().ifPresent(error -> {
		  	        String errMsg = error.getError().getMessage();
		  	        int errPosition = error.getInputPosition();
		  	        String name = processingRecordList.get(errPosition).getValue("name__v", ValueType.STRING);
		  	        
		  	        logService.info("Unable to create '" + processingRecordList.get(errPosition).getObjectName() + "' record: '" +
	        	                  name + "' because of '" + errMsg + "'.");
		   		});
		   }).execute();
		    
		   logService.info(message.toString());
		}
    }
    
    
    
    
    
//Processes a message from the queue and makes a HTTP callback to the source vault.
//This processing searches for an existing record based on the incoming message which has the source record ID.
//Once a record is found, an API VQL query is made to the source vault to grab data from the source record.
//This queried information is added to the target record.
//After it is updated, an API record update to sent to the source vault to update the `integration_status__c`:
//     `successfully_processed__c` - if the query and update are successful
//     `processing_failed__c`       - if the query or update are unsuccessful
    
    public static void callbackMessageType(Message message, MessageContext context) {
    	
    	LogService logService = ServiceLocator.locate(LogService.class);
    	RecordService recordService = ServiceLocator.locate(RecordService.class);
    	QueryService queryService = ServiceLocator.locate(QueryService.class);
    	
        //Get an instance of Job for invoking user actions, such as changing state and starting workflow
        JobService jobService = ServiceLocator.locate(JobService.class);
        JobParameters jobParameters = jobService.newJobParameters("record_user_action__v");
        
    	Map<String,String> incomingMessageMap = VaultCollections.newMap();
    	Map<String,Record> callBackMessageMap = VaultCollections.newMap();
    	Map<String,String> sourceToTargetIdMap = VaultCollections.newMap();
    	
    	
    	Map<String,String> destinationObjectMap = VaultCollections.newMap();
    	destinationObjectMap.put("purchase_order__c", "vsdk_purchase_order__c");
    	
    	List<Record> processingRecordList = VaultCollections.newList();
    	List<Record> deletedRecordList = VaultCollections.newList();
    	
    	//Streams require final values - used later to determine if a query applies or not.
    	final String messageState = message.getAttribute("state", MessageAttributeValueType.STRING).toString().toLowerCase();
    	final String destinationObject = destinationObjectMap.get(message.getAttribute("type", MessageAttributeValueType.STRING).toString().toLowerCase());
    	
    	logService.info("State: " + messageState);
    	
    	//Retrieve all items in the Message and put them in a Map. 
    	//The map is used to determine if the code wasn't able to find existing records.
    	for (String sourceRecordId : message.getMessageItems()) {
            incomingMessageMap.put(sourceRecordId, "true");
            logService.info("Incoming message item: " + sourceRecordId);
    	}
    
    	//Query to see any incoming IDs match to any existing vsdk_purchase_order__c records.
	    String query = "select id, source_record__c from " + destinationObject 
	    		     + " where source_record__c contains ('" + String.join("','", incomingMessageMap.keySet())  + "')";
	    QueryResponse queryResponse = queryService.query(query);
	    
	    logService.info("Query: " + query);
	    
		queryResponse.streamResults().forEach(qr -> {
	        String id = qr.getValue("id", ValueType.STRING);
	        String source_id = qr.getValue("source_record__c", ValueType.STRING);
	        Record recordUpdate = recordService.newRecordWithId(destinationObject, id);
	        
	        logService.info("Found existing record with ID: " + id);
	        
	        //The callBackMesasgeMap is used to query records from the source system and populate values in the target
	        //The sourceToTargetIdMap is used to populate integration status on the source after the target record is updated.
	        if (messageState.equals("approved")) {	
	        	callBackMessageMap.put(source_id, recordUpdate);   
	        	sourceToTargetIdMap.put(id, source_id);
	        }
	        //If the incoming message is marked as `deleted` and the query finds match, add the record to a delete list.
	        else if (messageState.equals("deleted")){
	        	deletedRecordList.add(recordUpdate);
	        }
	        
	        incomingMessageMap.remove(source_id);       
	    });
		queryResponse = null;
	    
		//If there are `approved` message items that don't already exist in the target vault, then go ahead and create them.
		if (messageState.equals("approved")){
			for (String key : incomingMessageMap.keySet()) {
		    	Record newSparkRecord = recordService.newRecord(destinationObject);
		    	
		    	newSparkRecord.setValue("source_vault__c", context.getRemoteVaultId().toString());
		    	newSparkRecord.setValue("source_record__c", key);
		    	newSparkRecord.setValue("integration_status__c", VaultCollections.asList("message_received__c"));
		    	
		    	callBackMessageMap.put(key, newSparkRecord);
			}
		}
		incomingMessageMap.clear();
		
        //When the query returned results or new approved records were created, go ahead and run a `HttpService` query callout.
		//This retrieves additional data from the source vault and populates the data into the target vault.
		//Add the updated records to a list that is processed with `batchSaveRecords`
		if (callBackMessageMap.size() > 0) {
			HttpCallouts.v2vHttpQuery(callBackMessageMap);
			processingRecordList.addAll(callBackMessageMap.values());
			callBackMessageMap.clear();
		}
		
		if (messageState.equals("deleted")) {
			if (deletedRecordList.size() > 0) {
				recordService.batchDeleteRecords(deletedRecordList).rollbackOnErrors().execute();
			}
		}
		else {
			if (processingRecordList.size() > 0) {
				recordService.batchSaveRecords(processingRecordList).onSuccesses(successMessage -> {
			    	
			    	List<Record> successfulRecords = VaultCollections.newList();
			    	List<String> successfulIdList = VaultCollections.newList();
			    	successMessage.stream().forEach(positionalRecordId -> {
			    		String targetId = positionalRecordId.getRecordId();
			    		Record record = recordService.newRecordWithId(destinationObject, positionalRecordId.getRecordId());
			    		
			    		if (sourceToTargetIdMap.containsKey(targetId)) {
			    			successfulIdList.add(sourceToTargetIdMap.get(targetId));
			    		}
			    		
			    		successfulRecords.add(record);
			    	});
			    	
			    	if (messageState.equals("approved")) {
				    	//If it is an approved record that is successfully updated, make a callback to the source vault and
				    	//update the source record with a "successfully_processed__c" integration status.
			        	HttpCallouts.v2vHttpUpdate(successfulIdList, "successfully_processed__c");
			        	jobParameters.setValue("user_action_name", "start_vsdk_po_workflow_useraction__c");
			            jobParameters.setValue("records",successfulRecords);
			            jobService.run(jobParameters);
			        }
			        
			   	}).onErrors(batchOperationErrors -> {
			   		
			    	List<String> failedIdList = VaultCollections.newList();
			   		
			   		batchOperationErrors.stream().findFirst().ifPresent(error -> {
			  	        String errMsg = error.getError().getMessage();
			  	        int errPosition = error.getInputPosition();
			  	        String name = processingRecordList.get(errPosition).getValue("name__v", ValueType.STRING);
			  	        String targetId = processingRecordList.get(errPosition).getValue("id", ValueType.STRING);
			  	        
			  	        logService.info("Unable to create '" + processingRecordList.get(errPosition).getObjectName() + "' record: '" +
		        	                  name + "' because of '" + errMsg + "'.");
			  	        
			  	        if (sourceToTargetIdMap.containsKey(targetId)) {
			  	        	failedIdList.add(sourceToTargetIdMap.get(targetId));
			    		}
			   		});
					
			    	if (messageState.equals("approved")) {
				    	//If it is an approved record that failed to update, make a callback to the source vault and
				    	//update the source record with a "processing_failed__c" integration status.
			        	HttpCallouts.v2vHttpUpdate(failedIdList, "processing_failed__c");
			        }
			   }).execute();
			    
			   logService.info(message.toString());
			}
		}
    	
    }
    
    //Inner public class created to implement Vault Java SDK Http Callouts.
    public static class HttpCallouts {
    	public HttpCallouts() {}
    	
    	//Input recordsToCheck Map contains a key (source ID) : value (record being updated) pair. 
    	//The keys are used to query the source system for field information.
    	//The values are used to update the target records with the retrieved field information.
	    public static void v2vHttpQuery(Map<String,Record> recordsToCheck) {
	    	
	    	LogService logService = ServiceLocator.locate(LogService.class);
	    	
	    	//This is a vault to vault Http Request to the `vsdk_v2v_connection`
	    	HttpService httpService = ServiceLocator.locate(HttpService.class);
			HttpRequest request = httpService.newHttpRequest("vsdk_v2v_connection");
			
			List<String> recordIds = VaultCollections.newList();
	
			//The configured connection provides the full DNS name. 
			//For the path, you only need to append the API endpoint after the DNS.
			//The query endpoint takes a POST where the BODY is the query itself.
			request.setMethod(HttpMethod.POST);
			request.appendPath("/api/v19.1/query");
			request.setHeader("Content-Type", "application/x-www-form-urlencoded");
			String query = "select id, "
						+ "comments__c, "
						+ "approval_date__c,"
						+ "model__c, "
						+ "(select first_name__c, last_name__c, address__c from bike_store_user__cr)"
						+ "from vsdk_bike_store__c where id contains ('" + String.join("','", recordsToCheck.keySet()) + "')";
			request.setBodyParam("q", query);
			
			
			//Below is an example VQL query.
			
			//		    {
			//	        "responseStatus": "SUCCESS",
			//	        "responseDetails": {
			//	            "limit": 1000,
			//	            "offset": 0,
			//	            "size": 1,
			//	            "total": 1
			//	        },
			//	        "data": [
			//	            {
			//	                "id": "VXX000000000XX1",
			//	                "comments__c": "This is a test comment.",
			//	                "approval_date__c": "2019-03-28",
			//	                "model__c": [
			//	                    "mountain_bike__c"
			//	                ],
			//	                "bike_store_user__cr": {
			//	                    "responseDetails": {
			//	                        "limit": 250,
			//	                        "offset": 0,
			//	                        "size": 1,
			//	                        "total": 1
			//	                    },
			//	                    "data": [
			//	                        {
			//	                            "first_name__c": "Test",
			//	                            "last_name__c": "User",
			//	                            "address__c": "123 Test Street Pleasanton, CA"
			//	                        }
			//	                    ]
			//	                }
			//	            }
			//	        ]
			//	    }
			
			
			//Send the request the source vault. The response received back should be a JSON response.
			//First, the response is parsed into a `JsonData` object
			//From the response, the `getJsonObject()` will get the response as a parseable `JsonObject`
			//    * Here the `getValue` method can be used to retrieve `responseStatus`, `responseDetails`, and `data`
			//The `data` element is an array of JSON data. This is parsed into a `JsonArray` object.
			//    * Each queried record is returned as an element of the array and must be parsed into a `JsonObject`. 
			//    * Individual fields can then be retrieved from each `JsonObject` that is in the `JsonArray`.
			
			httpService.send(request, HttpResponseBodyValueType.JSONDATA)
			.onSuccess(httpResponse -> {
				
				JsonData response = httpResponse.getResponseBody();
				
				if (response.isValidJson()) {
					String responseStatus = response.getJsonObject().getValue("responseStatus", JsonValueType.STRING);
					
					if (responseStatus.equals("SUCCESS")) {
						JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
						
						logService.info("HTTP Query Request: SUCCESS");
						
						//Retrieve each record returned from the VQL query.
						//Each element of the returned `data` JsonArray is a record with it's queried fields.
						for (int i = 0; i < data.getSize();i++) {
							JsonObject queryRecord = data.getValue(i, JsonValueType.OBJECT);
							
							String sourceId = queryRecord.getValue("id", JsonValueType.STRING);
							String model = queryRecord.getValue("model__c", JsonValueType.ARRAY).getValue(0, JsonValueType.STRING);
							String comments = queryRecord.getValue("comments__c", JsonValueType.STRING);
							String approvalDate  = queryRecord.getValue("approval_date__c", JsonValueType.STRING);
							
							//The bike_store_user__cr subquery has to be retrieved via a JsonObject then JsonArray.
							//This is basically an embedded JSON response.
							JsonObject subquery = queryRecord.getValue("bike_store_user__cr", JsonValueType.OBJECT);
							JsonArray subqueryData = subquery.getValue("data", JsonValueType.ARRAY);
							JsonObject subqueryRecord = subqueryData.getValue(0, JsonValueType.OBJECT);
							
							String firstName = subqueryRecord.getValue("first_name__c", JsonValueType.STRING);
							String lastName = subqueryRecord.getValue("last_name__c", JsonValueType.STRING);
							String address = subqueryRecord.getValue("address__c", JsonValueType.STRING);
							
							//With all the necessary data now retrieved, use the sourceId of the Http Response to populate the 
							//correct data record on the target vault.
							recordsToCheck.get(sourceId).setValue("comments__c", comments);
							recordsToCheck.get(sourceId).setValue("model__c", VaultCollections.asList(model));
							recordsToCheck.get(sourceId).setValue("first_name__c", firstName);
							recordsToCheck.get(sourceId).setValue("last_name__c", lastName);
							recordsToCheck.get(sourceId).setValue("address__c", address);
							recordsToCheck.get(sourceId).setValue("approval_date__c", LocalDate.parse(approvalDate));
							recordsToCheck.get(sourceId).setValue("integration_status__c", VaultCollections.asList("successfully_processed__c"));
							
							recordIds.add(sourceId);
							
							subquery = null;
							subqueryData = null;
							subqueryRecord = null;
						}
						data = null;	
					}
					else {
						for (String key : recordsToCheck.keySet()) {
							recordsToCheck.get(key).setValue("integration_status__c", VaultCollections.asList("processing_failed__c"));
						}
					}
					response = null;
				}
				else {
					logService.info("v2vHttpUpdate error: Received a non-JSON response.");
				}
			})
			.onError(httpOperationError -> {
				
				for (String key : recordsToCheck.keySet()) {
					recordsToCheck.get(key).setValue("integration_status__c", VaultCollections.asList("processing_failed__c"));
				}
			}).execute();
			
			request = null;
			
	    }
	    
    	//The recordsUpdate list parameter contains the affected source Ids. 
    	//The source Ids are used to build a batch of vsdk_bike_store__c updates for the source system.
	    //The Update Record API takes JSON data as the body of the HttpRequest, so we can
	    //  build a Json request with the `JsonObjectBuilder` and `JsonArrayBuilder`
	    public static void v2vHttpUpdate(List<String> recordsUpdate, String status) {
	    	
	    	HttpService httpService = ServiceLocator.locate(HttpService.class);
	    	LogService logService = ServiceLocator.locate(LogService.class);
	    	
	    	HttpRequest request = httpService.newHttpRequest("vsdk_v2v_connection");
	    	
			JsonService jsonService = ServiceLocator.locate(JsonService.class);
			JsonObjectBuilder jsonObjectBuilder = jsonService.newJsonObjectBuilder();
			JsonArrayBuilder jsonArrayBuilder = jsonService.newJsonArrayBuilder();
			
			//The Update Object Record API takes JSON data as input.
			//The input format is an array of Json objects.
			//Use the `JsonObjectBuilder` to build the individual Json Objects with the necessary updates.
			//Then add the resulting `JsonObject` objects to the `JsonArrayBuilder`
			for (String value : recordsUpdate) {
				
				JsonObject inputJsonObject = jsonObjectBuilder.setValue("id", value)
						   .setValue("integration_status__c", status)
						   .build();
				jsonArrayBuilder.add(inputJsonObject);
				
			}
			
			//Once all the Json objects are added to the `JsonArray`, use the `build` method to generate the array.
			JsonArray inputJsonArray = jsonArrayBuilder.build();

			if (inputJsonArray.getSize() > 0) {
				request.setMethod(HttpMethod.PUT);
				request.appendPath("/api/v19.1/vobjects/vsdk_bike_store__c");
				request.setHeader("Content-Type", "application/json");
				request.setBody(inputJsonArray);
				
				httpService.send(request, HttpResponseBodyValueType.JSONDATA)
				.onSuccess(httpResponse -> {
					
					JsonData response = httpResponse.getResponseBody();
					
					if (response.isValidJson()) {
						String responseStatus = response.getJsonObject().getValue("responseStatus", JsonValueType.STRING);
						
						if (responseStatus.equals("SUCCESS")) {
							JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
							
							//Retrieve the results for each record that was updated.
							//Each element of the returned `data` JsonArray is the results of a single updated record.
							for (int i = 0; i <= data.getSize()-1;i++) {
								JsonObject recordResponse = data.getValue(i, JsonValueType.OBJECT);
								String recordStatus = recordResponse.getValue("responseStatus", JsonValueType.STRING);
								JsonObject recordData = recordResponse.getValue("data", JsonValueType.OBJECT);
								String recordId = recordData.getValue("id", JsonValueType.STRING);
								
								logService.info("HTTP Update Request " + recordStatus + ": " + recordId);
							}
							data = null;
						}
						response = null;
					}
					else {
						logService.info("v2vHttpUpdate error: Received a non-JSON response.");
					}	
				})
				.onError(httpOperationError -> {
					logService.info(httpOperationError.getMessage());
					logService.info(httpOperationError.getHttpResponse().getResponseBody());
				}).execute();
				
				request = null;
				inputJsonArray = null;
			}
	    }
	    
	    
	    //Opens a local connection in the target vault to create a crosslink document.
	    //This runs as the Message Processor User - this user will require access to the source vault document.
	    //
	    // **** NOTE ****
	    //If your vault has additional required fields, they will need to be set with `setBodyParam`
	    //
	    public static void localCreateCrosslink(Message message, MessageContext context) {
		   
	    	LogService logService = ServiceLocator.locate(LogService.class);
	    	HttpService httpService = ServiceLocator.locate(HttpService.class);
		   
			String docId = context.getMessage().getAttribute("docId",MessageAttributeValueType.STRING);
			String docName = context.getMessage().getAttribute("docName",MessageAttributeValueType.STRING);
			String type =context.getMessage().getAttribute("type",MessageAttributeValueType.STRING);
			String remoteVaultId = context.getRemoteVaultId();
			
			logService.info("Creating Crosslink for document {} : {} in source vault {}", docId, docName, remoteVaultId);
		   
			HttpRequest request = httpService.newLocalHttpRequest()
	                .setMethod(HttpMethod.POST)
	                .appendPath("/api/v19.1/objects/documents")
	                .setBodyParam("source_document_id__v",docId)
	                .setBodyParam("source_vault_id__v", remoteVaultId)
	                .setBodyParam("source_binding_rule__v", "Latest version")
	                .setBodyParam("name__v",docName)
	                .setBodyParam("type__v",type)
	                .setBodyParam("lifecycle__v", "vSDK Spark Document Lifecycle");

	        httpService.send(request, HttpResponseBodyValueType.STRING)
                .onSuccess(response -> {
                    int responseCode = response.getHttpStatusCode();
                    logService.info("RESPONSE: " + responseCode);
                    logService.info("RESPONSE: " + response.getResponseBody());
                })
                .onError(httpOperationError -> {
                    int responseCode = httpOperationError.getHttpResponse().getHttpStatusCode();
                    logService.info("RESPONSE: " + responseCode);
                    logService.info(httpOperationError.getMessage());
                    logService.info(httpOperationError.getHttpResponse().getResponseBody());
                })
                .execute();
	    }
    }
}