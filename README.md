# Vault Java SDK Sample - vsdk-spark-sample

**Please see the [project wiki](https://github.com/veeva/vsdk-spark-v2v-sample/wiki) for a detailed walkthrough.**

The **vsdk-spark-v2v-sample** project covers the use of Spark Messaging to propagate messages from one vault (source) to another vault (target). The project will step through:

* Setup of the necessary Vault to Vault components
    * Vault _Connection_ records
    * Vault _Queues_ for the inbound and outbound Spark Messages
    * Various Vault components for the sample project
* Sample Code for:
    * Basic Message without a HTTP Callback - sends a Spark Message from a _Bike Store_ record in the source vault that creates a blank _Purchase Order_ record in the target vault.
    * Message with HTTP Callback - approve a _Bike Store_ record in the source vault and then send a message from a source vault to a target vault. This initiates an HTTP Callout in the target vault using the Vault API to query for more information the source vault. 
    * Create a Crosslink Document - send a Spark Message with document details from a source vault to a target vault. This initiates a local HTTP Callout in the target vault that creates a crosslink document against the document in the source vault.

**You will need two sandbox vaults to run through this project.**

## How to import

Import as a Maven project. This will automatically pull in the required Vault Java SDK dependencies. 

For Intellij this is done by:
- File -> Open -> Navigate to project folder -> Select the 'pom.xml' file -> Open as Project

For Eclipse this is done by:
- File -> Import -> Maven -> Existing Maven Projects -> Navigate to project folder -> Select the 'pom.xml' file
	    
## License

This code serves as an example and is not meant for production use.

Copyright 2019 Veeva Systems Inc.
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
  