{
    "contract_id": "AXAGBSDC-e2068e57-5b86-4d67-9a75-89e0da50351c",
    "name": "Guidewire Motors - Test Product",
    "domain": "DAAS DMG",
    "version": "1.0",
    "dataset": {
        "name": "az__guidewire_motor__azureagbsdmgcontainer",
        "logicalType": "object",
        "physicalType": "table",
        "description": "This dataset contains comprehensive information on motor Claims, coverage details, and claims, including financials, policy statuses, and provider information.",
        "dataGranularityDescription": "",
        "schema": {
            "type": "object",
            "properties": [
                {
                    "physicalName": "Allowable_Schedule",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Address",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Bill_Method",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Coverage_Type",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Default_Copayment",
                    "dataType": "int",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "City",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Effective_Date",
                    "dataType": "date",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": "",
                    "format": "YYYY-MM-DD"
                },
                {
                    "physicalName": "Insurance_Group",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Expiration_Date",
                    "dataType": "date",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": "",
                    "format": "YYYY-MM-DD"
                },
                {
                    "physicalName": "Is_Medicaid_Plan",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Is_Capitated_Plan",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Payor_ID",
                    "dataType": "int",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Insurer_Code",
                    "dataType": "int",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Policy_Description",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Policy_Date",
                    "dataType": "date",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": "",
                    "format": "YYYY-MM-DD"
                },
                {
                    "physicalName": "Policy_Number",
                    "dataType": "int",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Premium_Amount",
                    "dataType": "double",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Policy_Status",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Policyholder_Name",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Zip_Code",
                    "dataType": "int",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "State",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Short_Name",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "row_id",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Claim_Amount",
                    "dataType": "double",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Deductible_Amount",
                    "dataType": "int",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                },
                {
                    "physicalName": "Brokerage_Cost",
                    "dataType": "string",
                    "attributeName": "",
                    "criticalDataElement": "Y",
                    "description": ""
                }
            ]
        }
    },
    "servers": [
        {
            "server": "Azure Cloud Storage",
            "type": "Cloud Storage",
            "config": {
                "SOURCE_CONNECTION_STRING": "gAAAAABpIJSMrqRV7J8k-EdtSLm5pEFrtp2yNIe-1G1_U-dyoJouZ5V3swCwMhEHbZkSAoNl1iL26yt2DwFSZYtcrP6BoKvjCZ3dr77lG4AC8RIEyb2BV4hu5pShy0z_QwUcUISuNnWELnyI6a9HPTJ8xKA9x3H6fo5O0ZJK1PMUVhXS2A2Hq06NCkLiyjJ1FUgyII298vYb",
                "file_path": "abfss://datagovernance@zabsdaasdv01ae1storg01.dfs.core.windows.net/Ingestion/Source/Guidewire/guidewire_motor.csv",
                "account_name": "zabsdaasdv01ae1storg01",
                "container_name": "datagovernance",
                "path_prefix": "",
                "auth_mode": "account-key"
            }
        }
    ],
    "quality": {
        "rules": {
            "completeness": [
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Insurance_Group",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Expiration_Date",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Is_Medicaid_Plan",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Is_Capitated_Plan",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Coverage_Type",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Policyholder_Name",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Zip_Code",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "State",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Short_Name",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Payor_ID",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Insurer_Code",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Policy_Description",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Allowable_Schedule",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Address",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Bill_Method",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Default_Copayment",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "City",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Effective_Date",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Policy_Date",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Policy_Number",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Premium_Amount",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Policy_Status",
                    "ruleCondition": "IS NOT NULL",
                    "ruleCriterion": "IS NOT NULL",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                }
            ],
            "validity": [
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Effective_Date",
                    "ruleCondition": "Format",
                    "ruleCriterion": "YYYY-MM-DD",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Bill_Method",
                    "ruleCondition": "Contains",
                    "ruleCriterion": "['agency bill','company bill']",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Coverage_Type",
                    "ruleCondition": "Contains",
                    "ruleCriterion": "['auto','health','home','life','travel']",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Policy_Number",
                    "ruleCondition": "Length",
                    "ruleCriterion": "<= 6",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                }
            ],
            "accuracy": [],
            "uniqueness": [
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Effective_Date",
                    "ruleCondition": "IS UNIQUE",
                    "ruleCriterion": "IS UNIQUE",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                },
                {
                    "ruleName": "",
                    "ruleDescription": "",
                    "column_name": "Policy_Number",
                    "ruleCondition": "IS UNIQUE",
                    "ruleCriterion": "IS UNIQUE",
                    "ruleErrorThreshold": 5,
                    "weight": 1
                }
            ]
        }
    },
    "sla": {
        "availability": "99.9%",
        "support": {
            "source_owner_email": "jogesh.rajiyan@axa.com",
            "tech_support_email": "jogesh.rajiyan@axa.com"
        },
        "slaProperties": {
            "timeOfAvailability": "10:00 AM UTC",
            "latency": "1h",
            "frequency": "1d",
            "retention": "2y"
        }
    },
    "changes": "Initial version",
    "contractCreationDate": "2026-02-27",
    "contractLastUpdatedDate": "2026-02-27"
}

Now suggest me a way where I can fill in "dataGranularityDescription", properties["attributeName"], properties["description"],"ruleName", "ruleDescription" using ML models in Databricks 16.4 cluster
