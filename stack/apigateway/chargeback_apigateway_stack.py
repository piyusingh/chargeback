import json
from aws_cdk.aws_kms import Key
from aws_cdk import (
                     aws_lambda as lambda_,
                     aws_apigateway as apigw,
                     aws_certificatemanager as acm,
                     aws_logs as logs,
                     aws_iam
                     )


import aws_cdk as cdk
from constructs import Construct


class ChargebackApigatewayStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,raw_config,**kwargs):
        super().__init__(scope,construct_id, **kwargs)
        
        #Importing lambdas       
        import_trigger_lambda_arn = cdk.Fn.import_value(shared_value_to_import="Chargeback-triggerStateMachine-export")      
        triggerStateMachine=lambda_.Function.from_function_arn(self,"Chargeback_TriggerStateMachine_Import",
                                                                function_arn =import_trigger_lambda_arn)
        self.target_api = apigw.RestApi(self,'Chargeback',
                                        rest_api_name='Chargeback',
                                        endpoint_types=[apigw.EndpointType.REGIONAL],
                                        deploy_options=apigw.StageOptions(
                                            stage_name=raw_config.apigateway_info["stage_name"],
                                            throttling_burst_limit=raw_config.apigateway_info["throttling_burst_limit"],
                                            throttling_rate_limit=raw_config.apigateway_info["throttling_rate_limit"],
                                            data_trace_enabled = True,
                                            logging_level  = apigw.MethodLoggingLevel.INFO,
                                            tracing_enabled = True,
                                        ))

        violations ={
                "schema": apigw.JsonSchemaVersion.DRAFT4,
                "title": "Violations",
                "type": apigw.JsonSchemaType.OBJECT,  
                "properties": {
                    "occurrenceDate": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "convictionDate": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "incidentCategory": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    }
            }
        }

        Violations= apigw.Model(self,"Violations Model",
            rest_api= self.target_api,
            model_name="Violations",
            content_type="application/json",
            schema=violations
            )
       
        driver={
             "schema": apigw.JsonSchemaVersion.DRAFT4,
                "title": "Driver",
                "type": apigw.JsonSchemaType.OBJECT,  
                "required":[
                    "violation"
                    ],
                "properties":{
                    "firstName": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "lastName": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "DOB": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "driverStatus": {
                    "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]  
                    },
                    "licenseType": {
                    "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]  
                    },
                    "licenseNumber": {
                    "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]  
                    },
                    "violation": {
                        "type": apigw.JsonSchemaType.ARRAY,
                        "items":{
                            "ref":"https://apigateway.amazonaws.com/restapis/" + self.target_api.rest_api_id + "/models/Violations"
                        }
                    }
                   
                }
             }
       
        Driver= apigw.Model(self, "Driver Model",
            rest_api= self.target_api,
            model_name="Driver",
            content_type="application/json",
            schema=driver
            )
        Driver.node.add_dependency(Violations)


        commonElements={
                "schema": apigw.JsonSchemaVersion.DRAFT4,
                "title": "CommonElements",
                "type": apigw.JsonSchemaType.OBJECT,  
                "properties": {
                    "producerCode": {
                        "type": apigw.JsonSchemaType.STRING
                    },
                    "lob": {
                        "type": apigw.JsonSchemaType.STRING
                    },
                    "baseState": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "quoteID": {
                        "type": apigw.JsonSchemaType.STRING
                    },
                    "totalOrderedDrivers": {
                    "type": apigw.JsonSchemaType.INTEGER
                    },
                    "orderedDriversForCurrReq": {
                    "type": apigw.JsonSchemaType.INTEGER
                    },
                    "firstOrderDate": {
                    "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "startDate": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "reportType": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "orderDate": {
                        "type": apigw.JsonSchemaType.STRING
                    },
                    "orderTime": {
                        "type": apigw.JsonSchemaType.STRING
                    },
                    "quoteDate": {
                        "type": apigw.JsonSchemaType.STRING
                    },
                    "quoteTime": {
                        "type": apigw.JsonSchemaType.STRING
                    },
                    "policyIssued":{
                        "type": apigw.JsonSchemaType.BOOLEAN  
                    },
                    "policyIssueDate": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "policyIssueTime": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "policyNumber": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "totalDrivers": {
                        "type": apigw.JsonSchemaType.INTEGER
                    },
                    "driver":{
                        "type":apigw.JsonSchemaType.ARRAY,
                        "items":{
                            "ref": "https://apigateway.amazonaws.com/restapis/" + self.target_api.rest_api_id + "/models/Driver"
                        }
                    }
                }
            }

        CommonElements= apigw.Model(self,"Common Elements Model",
            rest_api= self.target_api,
            model_name="CommonElements",
            content_type="application/json",
            schema=commonElements
            )

        CommonElements.node.add_dependency(Driver)    

        requestPayload={
                "schema": apigw.JsonSchemaVersion.DRAFT4,
                "title": "RequestModel",
                "type": apigw.JsonSchemaType.OBJECT,
                "properties": {
                    "chargeback": {
                    "ref": "https://apigateway.amazonaws.com/restapis/" + self.target_api.rest_api_id + "/models/CommonElements"
                }
                }
                }


        RequestModel= apigw.Model(self,"Chargeback Request Payload Model",
            rest_api= self.target_api,
            model_name="RequestModel",
            content_type="application/json",
            schema=requestPayload
            )

        RequestModel.node.add_dependency(CommonElements)    

        RequestGetChargeback= apigw.Model(self,"Chargeback Get Payload Model",
            rest_api= self.target_api,
            model_name="RequestGetChargeback",
            content_type="application/json",
            schema={
                "schema": apigw.JsonSchemaVersion.DRAFT4,
                "title": "GetChargeback",
                "type": apigw.JsonSchemaType.OBJECT,
                "required":[
                    "quoteID"
                    ],
                "properties":{
                    "quoteID": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    },
                    "reportType": {
                        "type": [apigw.JsonSchemaType.STRING,apigw.JsonSchemaType.NULL]
                    }
                   
                }
            }
            )                          

        # Create Usage Plan
        Usageplan = self.target_api.add_usage_plan("UsagePlan",
        name="Chargeback",
        throttle={
            "rate_limit": raw_config.apigateway_info["rate_limit"],
            "burst_limit": raw_config.apigateway_info["burst_limit"],
            },
        )
        # Create APIGateway key
        api_key = apigw.ApiKey(
            self,
            "api_key",
            api_key_name="Chargeback_API_Key"
        )
    
        Usageplan.add_api_stage(stage=self.target_api.deployment_stage)
        Usageplan.add_api_key(api_key)
     
        #Create a resource named "savechargeback" on the base API
        saveChargeback = self.target_api.root.add_resource('saveChargeback')


        # Create API Integration for savechargeback
        saveChargeback_lambda_integration = apigw.LambdaIntegration(triggerStateMachine,proxy=True, integration_responses=
                                                     [
                                                         {
                                                             'statusCode': '200',
                                                         }
                                                     ]
                                                    )
       

        ReqValidator = self.target_api.add_request_validator("requestValidator",
            validate_request_body= True,
            validate_request_parameters= True
        )

       
        method_saveChargeback = saveChargeback.add_method("POST",saveChargeback_lambda_integration,
            request_models={
                'application/json': RequestModel
            },
            request_parameters={
                "method.request.header.application-name": False,
                "method.request.header.lineofbusiness": False,
                "method.request.header.x-correlation-id": False
            },
            method_responses= [
                {
                    'statusCode': '200'
                }
            ],
            request_validator= ReqValidator,
            api_key_required=True
            )                      
                                                          
        #Create a resource named "getChargeback" on the base API
        getChargeback = self.target_api.root.add_resource('getChargeback')
        # Create API Integration for getViolation
        getChargeback_lambda_integration = apigw.LambdaIntegration(triggerStateMachine,proxy=True, integration_responses=
                                                     [
                                                         {
                                                             'statusCode': '200',
                                                         }
                                                     ]
                                                    )
        method_getChargeback = getChargeback.add_method('POST', getChargeback_lambda_integration,
                              request_models={
                                'application/json': RequestGetChargeback
                            },
                            request_parameters={
                                "method.request.header.application-name": False,
                                "method.request.header.lineofbusiness": False,
                                "method.request.header.x-correlation-id": False
                            },
                            method_responses= [
                                {
                                    'statusCode': '200'
                                }
                            ],
                            request_validator= ReqValidator,
                            api_key_required=True
                            )
        
        ##Adding lambda resource polices
        trigger_lambda_getChargeback_resource_policy = lambda_.CfnPermission(self,
										"trigger_lambda_getChargeback_resource_policy",
										principal = "apigateway.amazonaws.com",
										action = "lambda:InvokeFunction",
                                        function_name = triggerStateMachine.function_name,
										source_arn  = self.target_api.arn_for_execute_api(method = "POST",
																						path = "/getChargeback",
																						stage = self.target_api.deployment_stage.stage_name))
        
        trigger_lambda_saveChargeback_resource_policy = lambda_.CfnPermission(self,
										"trigger_lambda_saveChargeback_resource_policy",
										principal = "apigateway.amazonaws.com",
										action = "lambda:InvokeFunction",
                                        function_name = triggerStateMachine.function_name,
										source_arn  = self.target_api.arn_for_execute_api(method = "POST",
																						path = "/saveChargeback",
																						stage = self.target_api.deployment_stage.stage_name))