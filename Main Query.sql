with
QUERY
AS
(
select CIF.DT_INVOICE_DATE as "Invoice Date Time",
       NCD.GCI as "Billing Client ID",
       NCD.GCI || ' - ' || NCD.GCI_NAME as "GCI and Billing Client Name",
       NCD.GCI_NAME as "Billing Client Name",
       CIF.INVOICE_LINE_NUMBER AS "INVOICE EXPEDITOR LINE",
       CIF.INVOICE_NUMBER AS "INVOICE EXPEDITOR",
       CIF.BILLING_AMOUNT as "Billing Amount",
       CURR.CURRENCY_ISO_CODE as "Billing Currency",   
       BD.BILLING_CODE as "Billing Code",
       CIF.BILLING_LINE_DESCRIPTION as "Billing Line Description",
       CIF.CUSTOMER_INVOICE_TYPE as "Customer Invoice Type",
       CIF.INTERNAL_REFERENCE as "Internal Reference",
       CED.AVERAGE_EXCHANGE_RATE as "USD Average Exchange Rate",
       NCD.MASTER_CLIENT_NUMBER as "Billing Master Client Number",
       NCD.MASTER_CLIENT_NAME as "Billing Master Client Name",
       NBD.BRANCH_CODE as "Billing Branch Code",
       DD.DATE_YEAR_KEY as "Invoice Year",
       DD.DATE_MONTH_NAME as "Invoice Month",
       DD."DATE" as "Invoice Date",
       NBD.COUNTRY_NAME as "Billing Branch Country Name",
       CASE SUBSTRING(CIF.INTERNAL_REFERENCE, 1, 1)
when 'J' then 'Truck'
when 'H' then 'Truck'
when '1' THEN 'Truck'
when '2' THEN 'Air'
when '3' then 'Air'
when '4' then 'Air'
when '5' then 'Ocean'
when '6' then 'Ocean'
when '7' then 'Ocean'
when '8' then 'Truck'
when '9' then 'Distribution'
else 'Other'
END as "Ship Mode",
       NCD.USAGE_DISTINCTION_TYPE as "Billing Client Usage Distinction Type",
       CIF.BILLING_AMOUNT*CED.AVERAGE_EXCHANGE_RATE as "Billing Amount USD",
       case
when CIF.BILLING_LINE_DESCRIPTION like '%F2F%' then 'Y'
when CIF.BILLING_LINE_DESCRIPTION like '%FOREIGN TO FOREIGN%' then 'Y'
when NCD.USAGE_DISTINCTION_TYPE like 'F2F%' then 'Y'
else 'N'
end as "F2F Flag",
       case
when (case
when CIF.BILLING_LINE_DESCRIPTION like '%F2F%' then 'Y'
when CIF.BILLING_LINE_DESCRIPTION like '%FOREIGN TO FOREIGN%' then 'Y'
when NCD.USAGE_DISTINCTION_TYPE like 'F2F%' then 'Y'
else 'N'
end) = 'Y' and NCD.USAGE_DISTINCTION_TYPE <> 'F2FC'
then 'N'
else 'Y'
end as "Bill To Customer Flag",
CASE
        WHEN BD.BILLING_CODE IN ('2102','2123','2130','4000','4001','4002','4004','4030','4402','4408','4421','4422','4469','4506','4701','4709','AMSA','P/UT')
        THEN 'Freight'
        WHEN BD.BILLING_CODE IN ('2101')
        THEN 'Duty'
        WHEN (BD.BILLING_CODE = '2100' AND CIF.BILLING_LINE_DESCRIPTION = 'K84')
        THEN 'Duty' 
        WHEN BD.BILLING_CODE in ('0201','0701','2100','4003','4592')
        THEN 'Customs'
        WHEN BD.BILLING_CODE IN ('0130','0131','2106','4401','4403','4423','4429','4480','V093')
        THEN 'Delivery'
        WHEN BD.BILLING_CODE IN ('2115','4007','4024','4597','4643','4645','V163','V192','V197')
        THEN 'Destination Charges'
        WHEN BD.BILLING_CODE IN ('2118','4018','4031','4083','4087','4461','4462','4463','4703','4708','4754','V196')
        THEN 'Origin Fees'
        WHEN BD.BILLING_CODE IN ('2190','4590','V098','Z408')
        THEN 'Other'
ELSE 'Other'
END as "Charge Type",
MONTH(DD."DATE") as "Invoice Month Number"
from   EAD_FACT.CUSTOMER_INVOICE_FACT_V CIF
INNER JOIN DW.DATE_DIMENSION_V DD ON CIF.DT_INVOICE_DATE_KEY = DD.DATE_KEY
INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCD ON CIF.BILLING_CLIENT_KEY = NCD.CLIENT_KEY
INNER JOIN EAD_MD.CURRENCY_EXCHANGE_DIMENSION_V CED ON CED.DATE_MONTH_KEY = CIF.DT_REVENUE_MONTH_KEY and CED.CURRENCY_KEY = CIF.BILLING_CURRENCY_KEY
INNER JOIN DW.BILLING_DIMENSION_V BD ON CIF.BILLING_KEY = BD.BILLING_KEY
INNER JOIN DW.CURRENCY_DIMENSION_V CURR ON CIF.BILLING_CURRENCY_KEY = CURR.CURRENCY_KEY
INNER JOIN DW.NEW_BRANCH_DIMENSION_V NBD ON CIF.BILLING_BRANCH_KEY = NBD.BRANCH_KEY
where (CASE WHEN '$(Client Type)' = 'MASTER' THEN NCD.MASTER_CLIENT_NUMBER ELSE NCD.GCI  END) IN ('$(Master Client Number)')
and DD."DATE" BETWEEN '$(InitialDate)' and '$(EndDate)'  
and ( BD.BILLING_CODE  || NBD.COUNTRY_CODE != '2125MX')
),
IMPORT
AS 
(
SELECT MAX(NCD.MASTER_CLIENT_NUMBER) as "Importer Master Client Number",
       ISF.INVOICE_NUMBER as "Invoice Number",
       MAX(ISF.HOUSEBILL_REF) as "EI File Number",
       MAX(BOD.COUNTRY_NAME) as "Origin Branch Country Name",
       MAX(BDD.COUNTRY_NAME) as "Destination Branch Country Name",
       MAX(ISF.WEIGHT_CHARGEABLE) as "Weight Chargeable",
       MAX(ISF.WEIGHT_ACTUAL) as "Actual Weight",
       MAX(ISF.VOLUME) as Volume,          
       MAX(BOD.BRANCH_CODE) as "Origin Branch Code",
       MAX(BDD.BRANCH_CODE) as "Destination Branch Code",
       MAX(BOD.BRANCH_CODE) || '-' || MAX(BDD.BRANCH_CODE) AS "Lane",
       MAX(NCDSH.GCI_NAME) as "Shipper Name",
       MAX(NCDSH.GCI_ADDR_LINE1) as "Shipper Address Line 1",
       MAX(NCDSH.GCI_ADDR_LINE2) as "Shipper Address Line 2",
       MAX(NCDSH.GCI_STATE) as "Shipper State",
       MAX(NCDSH.GCI_CITY) as "Shipper City",
       MAX(NCDCON.GCI_NAME) as "Consignee Name",
       MAX(NCDCON.GCI_ADDR_LINE1) as "Consignee Address Line 1",
       MAX(NCDCON.GCI_ADDR_LINE2) as "Consignee Address Line 2",
       MAX(NCDCON.GCI_STATE) as "Consignee State",
       MAX(NCDCON.GCI_CITY) as "Consignee City",
       MAX(SLD.SERVICE_LEVEL_DESCRIPTION) as "Service Level Desc",
       MAX(ISF.DT_ACT_DEP_EVENT_TIME) as "Departure Event Date Time",
       MAX(SFD.SERVICE_TYPE_CODE) as "Service Type Code",       
       CASE WHEN MAX(EV.EVENT_CODE) = 'FRT' THEN MAX(EV.EVENT_TIME) END AS "FRT",
       CASE WHEN MAX(EV.EVENT_CODE) = 'POD' THEN MAX(EV.EVENT_TIME) END AS "POD",
       CASE WHEN MAX(EV.EVENT_CODE) = 'ADU' THEN MAX(EV.EVENT_TIME) END AS "ADU",                     
       CASE WHEN MAX(EV.EVENT_CODE) = 'FND' THEN MAX(EV.EVENT_TIME) END AS "FND",
       CASE WHEN MAX(EV.EVENT_CODE) = 'ADD' THEN MAX(EV.EVENT_TIME) END AS "ADD",       
       CASE WHEN MAX(EV.EVENT_CODE) = 'AFD' THEN MAX(EV.EVENT_TIME) END AS "AFD",              
       CASE WHEN MAX(EV.EVENT_CODE) = 'DLV' THEN MAX(EV.EVENT_TIME) END AS "DLV",      
       CASE WHEN MAX(EV.EVENT_CODE) = 'COB' THEN MAX(EV.EVENT_TIME) END AS "COB",                                  
       CASE WHEN MAX(EV.EVENT_CODE) = 'BKD' THEN MAX(EV.EVENT_TIME) END AS "BKD",                                                                                                     
       CASE WHEN MAX(EV.EVENT_CODE) = 'EDA' THEN MAX(EV.EVENT_TIME) END AS "EDA",                                                      
       CASE WHEN MAX(EV.EVENT_CODE) = 'EDI' THEN MAX(EV.EVENT_TIME) END AS "EDI",                                                      
       CASE WHEN MAX(EV.EVENT_CODE) = 'EDS' THEN MAX(EV.EVENT_TIME) END AS "EDS",                                                      
       MAX(CASE WHEN RF.REFERENCE_TYPE = 'IC' THEN RF.REFERENCE_NO END) AS "IC",
       MAX(CASE WHEN RF.REFERENCE_TYPE = 'PO' THEN RF.REFERENCE_NO END) AS "PO",  
       MAX(CASE WHEN RF.REFERENCE_TYPE = 'MB' THEN RF.REFERENCE_NO END) AS "MB",  
       MAX(CASE WHEN RF.REFERENCE_TYPE = 'DP' THEN RF.REFERENCE_NO END) as "DP",
       CASE WHEN MAX(CASE WHEN RF.REFERENCE_TYPE = 'IC' THEN RF.REFERENCE_NO END) IS NULL THEN 'N' ELSE 'Y' END AS "We Move",
       MAX(PORT_ORIGIN.PORT_CODE) as "Origin Port Code",
       MAX(PORT_DEST.PORT_CODE) as "Destination Port Code",       
       MAX(ISF.HAZARD_FLAG_YN) AS "Hazard Flag",
       MAX(BOD.REGION_CODE_1) as "Origin Branch Region Code",        
       MAX(BDD.REGION_CODE_1) as "Destination Branch Region Code",
       MAX(ISF.Quantity) as "QTY",
       MAX(CASE WHEN RF.REFERENCE_TYPE = 'CI' THEN RF.REFERENCE_NO END) AS "CI",  
       MAX(CASE WHEN RF.REFERENCE_TYPE = 'EN' THEN RF.REFERENCE_NO END) AS "EN"                                      
FROM   DW.NEW_CLIENT_DIMENSION_V NCD
       INNER JOIN EAD_FACT.IMPORT_SHIPMENT_FACT_V ISF ON NCD.CLIENT_KEY = ISF.IMPORTER_KEY               
       INNER JOIN DW.NEW_BRANCH_DIMENSION_V BOD ON BOD.BRANCH_KEY = ISF.BRANCH_ORIGIN_KEY
       INNER JOIN DW.NEW_BRANCH_DIMENSION_V BDD ON BDD.BRANCH_KEY = ISF.BRANCH_DESTINATION_KEY
       INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCDSH ON NCDSH.CLIENT_KEY = ISF.SHIPPER_KEY
       INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCDCON ON NCDCON.CLIENT_KEY = ISF.CONSIGNEE_KEY
       INNER JOIN EAD_MD.SERVICE_LEVEL_DIMENSION_V SLD ON SLD.SERVICE_LEVEL_KEY = ISF.SERVICE_LEVEL_KEY
       INNER JOIN EAD_MD.PORT_DIMENSION_V PORT_ORIGIN ON PORT_ORIGIN.PORT_KEY = ISF.PORT_ORIGIN_KEY
       INNER JOIN EAD_MD.PORT_DIMENSION_V PORT_DEST ON PORT_DEST.PORT_KEY = ISF.PORT_DESTINATION_KEY
       LEFT JOIN EAD_MD.SERVICE_TYPE_DIMENSION_V SFD  ON ISF.SERVICE_TYPE_KEY = SFD.SERVICE_TYPE_KEY        
       LEFT JOIN EAD_FACT.REFERENCE_FACT_V RF ON ISF.INVOICE_NUMBER = RF.INVOICE_NO AND  RF.REFERENCE_TYPE in ('IC','PO','CI','EN','MB','DP')
       LEFT JOIN EAD_FACT.EVENT_V EV ON ISF.INVOICE_NUMBER = EV.INVOICE_NO AND EV.DELETED_YN = 'N' AND EV.EVENT_CODE in ('FRT','ADU','COB','FND','ADD','AFD','DLV','BKD','EDA','EDI','EDE','EDS')                                                                                                                 
WHERE (CASE WHEN '$(Client Type)' = 'MASTER' THEN NCD.MASTER_CLIENT_NUMBER ELSE NCD.GCI END) IN ('$(Master Client Number)')
       
GROUP BY ISF.INVOICE_NUMBER
),



EXPO
AS
(
with
SO
AS
(
SELECT
        SF.MOVEMENT_ID as "SO EI File Number"	
	,COALESCE(MAX(SRF.SO_REFERENCES),MAX(CASE WHEN RF.REFERENCE_TYPE = 'SO' THEN RF.REFERENCE_NO END)) AS "SO"
        ,MAX(CASE WHEN RF.REFERENCE_TYPE = 'IC' THEN RF.REFERENCE_NO END) AS "IC"
        ,MAX(CASE WHEN RF.REFERENCE_TYPE = 'PO' THEN RF.REFERENCE_NO END) AS "PO" 
        ,CASE WHEN MAX(CASE WHEN RF.REFERENCE_TYPE = 'IC' THEN RF.REFERENCE_NO END) IS NULL THEN 'N' ELSE 'Y' END AS "We Move"
        ,CASE WHEN MAX(EV.EVENT_CODE) = 'EDA' THEN MAX(EV.EVENT_TIME) END AS "EDA"                                                      
        ,CASE WHEN MAX(EV.EVENT_CODE) = 'EDI' THEN MAX(EV.EVENT_TIME) END AS "EDI"                                                     
        ,CASE WHEN MAX(EV.EVENT_CODE) = 'EDS' THEN MAX(EV.EVENT_TIME) END AS "EDS"  
        ,COALESCE(MAX(SRF.CI_REFERENCES),MAX(CASE WHEN RF.REFERENCE_TYPE = 'CI' THEN RF.REFERENCE_NO END)) as "CI"  
        ,COALESCE(MAX(SRF.EN_REFERENCES),MAX(CASE WHEN RF.REFERENCE_TYPE = 'EN' THEN RF.REFERENCE_NO END)) as "EN"  
        ,MAX(CASE WHEN RF.REFERENCE_TYPE = 'MB' THEN RF.REFERENCE_NO END) as "MB"
        ,MAX(CASE WHEN RF.REFERENCE_TYPE = 'DP' THEN RF.REFERENCE_NO END) as "DP"
FROM   
        EAD_FACT.SHIPMENT_FACT_V SF
        INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCD ON SF.OUTBOUND_REVENUE_CUSTOMER_KEY = NCD.CLIENT_KEY
        INNER JOIN EAD_FACT.SHIPMENT_REFERENCE_FACT_V SRF ON SF.MOVEMENT_ID = SRF.MOVEMENT_ID
        INNER JOIN EAD_FACT.INVOICE_MOVEMENT_V IM ON SF.MOVEMENT_ID = IM.MOVEMENT_ID   
        LEFT JOIN EAD_FACT.EVENT_V EV on EV.invoice_no = IM.invoice_number and EV.event_code in ('EDI','EDA', 'EDS')     
        LEFT JOIN EAD_FACT.REFERENCE_FACT_V RF ON IM.INVOICE_NUMBER = RF.INVOICE_NO AND  RF.REFERENCE_TYPE in ('SO','IC','PO','CI','EN','MB','DP')        
WHERE   (CASE WHEN '$(Client Type)' = 'MASTER' THEN NCD.MASTER_CLIENT_NUMBER ELSE NCD.GCI END) IN ('$(Master Client Number)')
GROUP BY
SF.MOVEMENT_ID
),



EXP
AS
(select 
        NCD.MASTER_CLIENT_NUMBER as "Outbound Revenue Client Master Client Number",
        NCD.GCI as "Revenue GCI",
        SF.MOVEMENT_ID as "EI File Number",
        SF.WEIGHT_CHARGEABLE as "Weight Chargeable Exp",
        SF.WEIGHT_ACTUAL as "Actual Weight",
        SF.VOLUME as "Volume Exp",
        IM.INVOICE_NUMBER as "Invoice Number",
        BOD.COUNTRY_NAME as "Origin Branch Country Name",
        BDD.COUNTRY_NAME as "Destination Branch Country Name",
        BOD.BRANCH_CODE || '-' || BDD.BRANCH_CODE AS "Lane Exp"
        ,BOD.BRANCH_CODE AS "Origin Branch"
        ,BDD.BRANCH_CODE AS "Destination Branch"
        ,COALESCE (SFE.COB_EVENT_TIME_FIRST, SFE.COB_EVENT_TIME_EARLIEST) AS "COB"
        ,COALESCE (SFE.FRT_EVENT_TIME_FIRST) AS "FRT"
        ,SFE.ADU_EVENT_TIME_FIRST AS "ADU"
        ,SFE.AFD_EVENT_TIME_FIRST AS "AFD"
        ,SFE.DLV_EVENT_TIME_FIRST AS "DLV"         
        ,SFE.POD_EVENT_TIME_LATEST AS "POD"                      
        ,SFE.BKD_EVENT_TIME_FIRST AS "BKD"  
        ,NCDCON.GCI_NAME as "Consignee"
        ,NCDCON.GCI_ADDR_LINE1 as "Outbound Consignee Address Line 1"
        ,NCDCON.GCI_ADDR_LINE2 as "Outbound Consignee Address Line 2"
        ,NCDCON.GCI_STATE as "Outbound Consignee State Province"
        ,NCDCON.GCI_CITY as "Outbound Consignee City" 
        ,NCDSH.GCI_NAME as "Shipper"
        ,NCDSH.GCI_ADDR_LINE1 as "Outbound Shipper Address Line 1"
        ,NCDSH.GCI_ADDR_LINE2 as "Outbound Shipper Address Line 2"
        ,NCDSH.GCI_STATE as "Outbound Shipper State Province"
        ,NCDSH.GCI_CITY as "Outbound Shipper City"
        ,SLD.SERVICE_LEVEL_DESCRIPTION as "Service Level"
        ,OP.PORT_CODE AS "EXP Origin Port"
        ,DP.PORT_CODE AS "EXP Destination Port"                
        ,SF.HAZARD_FLAG_YN AS "Hazard Flag"
        ,BOD.REGION_CODE_1 as "Origin Branch Region Code"
        ,BDD.REGION_CODE_1 as "Destination Branch Region Code"                                      
        ,SRF.PO_REFERENCES AS "PO_BUENO"                
        ,SFD.SERVICE_TYPE_CODE as "Service Type Code"    
        ,MAX(SF.PIECES) as "QTY"               
from   
EAD_FACT.SHIPMENT_FACT_V SF
INNER JOIN EAD_FACT.SHIPMENT_FACT_EVENTS_TIMELINE_V SFE ON SF.SHIPMENT_FACT_KEY = SFE.SHIPMENT_FACT_KEY
INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCD ON SF.OUTBOUND_REVENUE_CUSTOMER_KEY = NCD.CLIENT_KEY
INNER JOIN DW.NEW_BRANCH_DIMENSION_V BOD ON SF.BRANCH_ORIGIN_KEY = BOD.BRANCH_KEY
INNER JOIN DW.NEW_BRANCH_DIMENSION_V BDD ON SF.BRANCH_DEST_KEY = BDD.BRANCH_KEY
INNER JOIN EAD_FACT.INVOICE_MOVEMENT_V IM ON SF.MOVEMENT_ID = IM.MOVEMENT_ID
INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCDCON ON SF.OUTBOUND_CONSIGNEE_KEY = NCDCON.CLIENT_KEY
INNER JOIN EAD_MD.SERVICE_LEVEL_DIMENSION_V SLD ON SF.SERVICE_LEVEL_KEY = SLD.SERVICE_LEVEL_KEY
INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCDSH ON SF.OUTBOUND_SHIPPER_KEY = NCDSH.CLIENT_KEY
INNER JOIN EAD_MD.PORT_DIMENSION_V OP ON SF.PORT_ORIGIN_KEY = OP.PORT_KEY
INNER JOIN EAD_MD.PORT_DIMENSION_V DP ON SF.PORT_DEST_KEY = DP.PORT_KEY
LEFT JOIN EAD_MD.SERVICE_TYPE_DIMENSION_V SFD ON SF.SERVICE_TYPE_KEY = SFD.SERVICE_TYPE_KEY 
LEFT JOIN EAD_FACT.SHIPMENT_REFERENCE_FACT_V SRF ON SF.MOVEMENT_ID = SRF.MOVEMENT_ID
where (CASE WHEN '$(Client Type)' = 'MASTER' THEN NCD.MASTER_CLIENT_NUMBER ELSE NCD.GCI END) IN ('$(Master Client Number)')
group by 
NCD.MASTER_CLIENT_NUMBER,NCD.GCI,SF.MOVEMENT_ID,SF.WEIGHT_CHARGEABLE,SF.WEIGHT_ACTUAL,
SF.VOLUME,IM.INVOICE_NUMBER,BOD.COUNTRY_NAME,BDD.COUNTRY_NAME,BOD.BRANCH_CODE,BDD.BRANCH_CODE,
SFE.COB_EVENT_TIME_FIRST,SFE.COB_EVENT_TIME_EARLIEST,SFE.FRT_EVENT_TIME_FIRST,SFE.ADU_EVENT_TIME_FIRST,
SFE.AFD_EVENT_TIME_FIRST,SFE.DLV_EVENT_TIME_FIRST,SFE.POD_EVENT_TIME_LATEST,SFE.BKD_EVENT_TIME_FIRST,NCDCON.GCI_NAME,
NCDCON.GCI_ADDR_LINE1,NCDCON.GCI_ADDR_LINE2 ,NCDCON.GCI_STATE,NCDCON.GCI_CITY,NCDSH.GCI_NAME,NCDSH.GCI_ADDR_LINE1,
NCDSH.GCI_ADDR_LINE2,NCDSH.GCI_STATE,NCDSH.GCI_CITY,SLD.SERVICE_LEVEL_DESCRIPTION,OP.PORT_CODE,DP.PORT_CODE,
SF.HAZARD_FLAG_YN ,BOD.REGION_CODE_1,BDD.REGION_CODE_1,SRF.PO_REFERENCES,SFD.SERVICE_TYPE_CODE
) 
SELECT *
FROM
EXP
INNER JOIN SO ON EXP."EI File Number" = SO."SO EI File Number"
),
CONTAINERS AS 
(
WITH CTN as (
SELECT SF.MOVEMENT_ID as "EI File Number",
lead(CSF.CONTAINER_NUMBER,0,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER) ||
coalesce('|' || lead(CSF.CONTAINER_NUMBER,1,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,2,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,3,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,4,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,5,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,6,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,7,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,8,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,9,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,10,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,11,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,12,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,13,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,14,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,15,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,16,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,17,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,18,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,19,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,20,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,21,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,22,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,23,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,24,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,25,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,26,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,27,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,28,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,29,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '')
                    || coalesce('|' || lead(CSF.CONTAINER_NUMBER,30,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER), '') as "Container Number",
                    COUNT(CSF.CONTAINER_NUMBER) Over (PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER DESC) as "Container Count",
                    Row_Number() Over (PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER) as "Row"

from   EAD_FACT.SHIPMENT_FACT_V SF      
INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCD                 ON SF.OUTBOUND_REVENUE_CUSTOMER_KEY = NCD.CLIENT_KEY
INNER JOIN EAD_FACT.CONTAINER_SHIPMENT_FACT_V CSF        ON SF.MOVEMENT_ID = CSF.MOVEMENT_ID
INNER JOIN EAD_MD.MOVE_TYPE_DIMENSION_V MTD              ON CSF.MOVE_TYPE_KEY = MTD.MOVE_TYPE_KEY
INNER JOIN EAD_MD.CONTAINER_TYPE_DIMENSION_V CTD         ON CSF.CONTAINER_TYPE_KEY = CTD.CONTAINER_TYPE_KEY
INNER JOIN EAD_MD.SHIP_MODE_DIMENSION_V SMD              ON SF.SHIP_MODE_KEY = SMD.SHIP_MODE_KEY 
WHERE 
(CASE WHEN '$(Client Type)' = 'MASTER' THEN NCD.MASTER_CLIENT_NUMBER ELSE NCD.GCI END) IN ('$(Master Client Number)') and 
SMD.SHIP_MODE_DESC = 'Ocean' 
order by SF.MOVEMENT_ID,Row_Number() Over (PARTITION BY SF.MOVEMENT_ID ORDER BY CSF.CONTAINER_NUMBER)),
CTN2 as (
SELECT SF.MOVEMENT_ID as "EI File Number",
lead(CTD.CONTAINER_TYPE,0,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE) ||
coalesce('|' || lead(CTD.CONTAINER_TYPE,1,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,2,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,3,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,4,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,5,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,6,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,7,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,8,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,9,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,10,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,11,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,12,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,13,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,14,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,15,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,16,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,17,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,18,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,19,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,20,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,21,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,22,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,23,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,24,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,25,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,26,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,27,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,28,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,29,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '')
                    || coalesce('|' || lead(CTD.CONTAINER_TYPE,30,NULL) over(PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE), '') as "Container Size",
                    Row_Number() Over (PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE) as "Row"

from   EAD_FACT.SHIPMENT_FACT_V SF      
INNER JOIN DW.NEW_CLIENT_DIMENSION_V NCD                 ON SF.OUTBOUND_REVENUE_CUSTOMER_KEY = NCD.CLIENT_KEY
INNER JOIN EAD_FACT.CONTAINER_SHIPMENT_FACT_V CSF        ON SF.MOVEMENT_ID = CSF.MOVEMENT_ID
INNER JOIN EAD_MD.MOVE_TYPE_DIMENSION_V MTD              ON CSF.MOVE_TYPE_KEY = MTD.MOVE_TYPE_KEY
INNER JOIN EAD_MD.CONTAINER_TYPE_DIMENSION_V CTD         ON CSF.CONTAINER_TYPE_KEY = CTD.CONTAINER_TYPE_KEY
INNER JOIN EAD_MD.SHIP_MODE_DIMENSION_V SMD              ON SF.SHIP_MODE_KEY = SMD.SHIP_MODE_KEY 
WHERE 
(CASE WHEN '$(Client Type)' = 'MASTER' THEN NCD.MASTER_CLIENT_NUMBER ELSE NCD.GCI END) IN ('$(Master Client Number)') and 
SMD.SHIP_MODE_DESC = 'Ocean' 
order by SF.MOVEMENT_ID,Row_Number() Over (PARTITION BY SF.MOVEMENT_ID ORDER BY CTD.CONTAINER_TYPE))

SELECT CTN."EI File Number",
CTN."Container Number" as "Containers",
CTN."Container Count" as "Container Count",
CTN2."Container Size" as "Container Sizes"
FROM CTN
INNER JOIN CTN2 on CTN2."EI File Number" = CTN."EI File Number" and CTN2."Row" = 1
wHERE CTN."Row" = 1
)

SELECT /*+label(iah-myrona_SpendRubicon_20180921)*/
QUERY."Invoice Date Time",
QUERY."INVOICE EXPEDITOR",
QUERY."INVOICE EXPEDITOR LINE", 
REPLACE(REPLACE(QUERY."Billing Line Description",'"',''),',',' ') as "Billing Line Description",
QUERY."Internal Reference",
QUERY."Billing Code",
QUERY."Invoice Date", 
CASE WHEN COUNT(DISTINCT COALESCE(EXPO."EI File Number", IMPORT."EI File Number")) > 1 THEN MAX(QUERY."Billing Amount") ELSE SUM(QUERY."Billing Amount") END as "Billing Amount",
AVG(QUERY."USD Average Exchange Rate") as "USD Average Exchange Rate",
CASE WHEN COUNT(DISTINCT COALESCE(EXPO."EI File Number", IMPORT."EI File Number")) > 1 THEN MAX(QUERY."Billing Amount USD") ELSE SUM(QUERY."Billing Amount USD") END  as "Billing Amount USD",
SUM (COALESCE(EXPO."Volume Exp", IMPORT."Volume")) as "Final Volume", 
SUM (COALESCE(EXPO."Weight Chargeable Exp", IMPORT."Weight Chargeable")) AS "Final ChWeight",
SUM (COALESCE(EXPO."Actual Weight",IMPORT."Actual Weight")) as "Actual Weight",
MAX(QUERY."Billing Client ID") AS "Billing Client ID" ,
MAX(QUERY."Billing Client Name") AS "Billing Client Name" ,
MAX(QUERY."GCI and Billing Client Name") as "GCI and Billing Client Name",
MAX(QUERY."Billing Currency") AS "Billing Currency",
MAX(QUERY."Billing Branch Code") AS "Billing Branch Code",
MAX(QUERY."Customer Invoice Type") AS "Customer Invoice Type", 
MAX(QUERY."Billing Master Client Number") AS "Billing Master Client Number", 
MAX(QUERY."Billing Master Client Name") AS "Billing Master Client Name",
MAX(QUERY."Invoice Year") AS "Invoice Year",
MAX(QUERY."Invoice Month") AS "Invoice Month",
MAX(QUERY."Billing Branch Country Name") AS "Billing Branch Country Name",
MAX (QUERY."Ship Mode") AS "Ship Mode",
MAX(QUERY."Billing Client Usage Distinction Type") AS "Billing Client Usage Distinction Type",
MAX(QUERY."F2F Flag") AS "F2F Flag",
case when MAX(QUERY."F2F Flag") = 'Y' and isnull(MAX(QUERY."Billing Client Usage Distinction Type"),'') <> 'F2FC' THEN 'N' ELSE 'Y' end as "Bill To Customer Flag",
MAX(QUERY."Charge Type") AS "Charge Type",
MAX (QUERY."Invoice Month Number") AS "Invoice Month Number",
MAX (COALESCE(EXPO."PO_BUENO", IMPORT."PO")) AS PO, 
MAX (COALESCE(EXPO."Origin Branch Country Name",IMPORT."Origin Branch Country Name")) AS "Origin Branch",
MAX (COALESCE(EXPO."Destination Branch Country Name", IMPORT."Destination Branch Country Name", QUERY."Billing Branch Country Name")) as "Destination Country", 
MAX (COALESCE(IMPORT."Service Type Code",EXPO."Service Type Code")) AS "Service Type Code",   
MAX (COALESCE(EXPO."Lane Exp", IMPORT."Lane")) AS "Final Lane", 
REPLACE(MAX(COALESCE(EXPO."Shipper", IMPORT."Shipper Name")),'"','')  as "Final Shipper", 
MAX (COALESCE(EXPO."EI File Number", IMPORT."EI File Number")) as "House Bill",
COUNT(DISTINCT COALESCE(EXPO."EI File Number", IMPORT."EI File Number")) as "HB Count",
REPLACE(MAX (COALESCE(EXPO."Consignee", IMPORT."Consignee Name")),'"','') as "Final Consignee", 
MAX (COALESCE(EXPO."Origin Branch", IMPORT."Origin Branch Code")) as "Final Origin Branch", 
MAX (COALESCE(EXPO."Destination Branch", IMPORT."Destination Branch Code")) as "Final Destination Branch",
MAX (COALESCE(EXPO."Service Level", IMPORT."Service Level Desc")) as "Final Service Level",
MAX (COALESCE(EXPO."We Move", IMPORT."We Move")) as "We Move YN", 
MAX (COALESCE(EXPO."FRT",IMPORT."FRT",IMPORT."FND")) AS "Freight", 
MAX (COALESCE(EXPO."COB", IMPORT."Departure Event Date Time")) as "Ship Date", 
MAX (COALESCE(EXPO."ADU", EXPO."ADU", EXPO."AFD",IMPORT."ADD",IMPORT."ADU", IMPORT."AFD")) AS "Arrival", 
MAX (COALESCE(EXPO."DLV",IMPORT."DLV")) AS "Delivery", 
COALESCE(MAX(EXPO."EDA"),MAX(IMPORT."EDA")) AS "EDA",                                                      
COALESCE(MAX(EXPO."EDI"),MAX(IMPORT."EDI")) AS "EDI",
COALESCE(MAX(EXPO."EDS"),MAX(IMPORT."EDS")) AS "EDS",                                                      
MAX (COALESCE(EXPO."POD",IMPORT."POD")) AS "Proof of Delivery",               
MAX (COALESCE(EXPO."BKD",IMPORT."BKD")) AS "BKD",                 
MAX (
CASE MONTH(COALESCE(EXPO."COB", IMPORT."Departure Event Date Time")) 
WHEN '1' THEN '01-Jan'
WHEN '2' THEN '02-Feb'
WHEN '3' THEN '03-Mar'
WHEN '4' THEN '04-Apr'
WHEN '5' THEN '05-May'
WHEN '6' THEN '06-Jun'
WHEN '7' THEN '07-Jul'
WHEN '8' THEN '08-Aug'
WHEN '9' THEN '09-Sept'
WHEN '10' THEN '10-Oct'
WHEN '11' THEN '11-Nov'
WHEN '12' THEN '12-Dec'
ELSE null
END )
as "Month", 
MAX ('Q' || EXTRACT(QUARTER FROM COALESCE(EXPO."COB", IMPORT."Departure Event Date Time")) ) AS "QUARTER",
MAX (YEAR(COALESCE(EXPO."COB", IMPORT."Departure Event Date Time")) ) AS "YEAR", 
REPLACE(REPLACE(MAX(COALESCE(EXPO."Outbound Shipper Address Line 1", IMPORT."Shipper Address Line 1")),'"',''),',',' ') as "Shipper Address1", 
REPLACE(REPLACE(MAX(COALESCE(EXPO."Outbound Shipper Address Line 2", IMPORT."Shipper Address Line 2")),'"',''),',',' ') as "Shipper Address2",
MAX (COALESCE(EXPO."Outbound Shipper State Province", IMPORT."Shipper State")) AS "Shipper_State", 
REPLACE(REPLACE(MAX(COALESCE(EXPO."Outbound Shipper City", IMPORT."Shipper City")),'"',''),',',' ') AS "Shipper_City", 
REPLACE(REPLACE(MAX(COALESCE(EXPO."Outbound Consignee Address Line 1", IMPORT."Consignee Address Line 1")),'"',''),',',' ') as "Consignee Address1", 
REPLACE(REPLACE(MAX(COALESCE(EXPO."Outbound Consignee Address Line 2", IMPORT."Consignee Address Line 2")),'"',''),',',' ') as "Consignee Address2", 
MAX (COALESCE(EXPO."Outbound Consignee State Province", IMPORT."Consignee State")) as "Consignee State",
REPLACE(REPLACE(MAX(COALESCE(EXPO."Outbound Consignee City", IMPORT."Consignee City")),'"',''),',',' ') as "Consignee_City", 
MAX (
CASE MONTH(QUERY."Invoice Date Time")
WHEN '1' THEN '01-Jan'
WHEN '2' THEN '02-Feb'
WHEN '3' THEN '03-Mar'
WHEN '4' THEN '04-Apr'
WHEN '5' THEN '05-May'
WHEN '6' THEN '06-Jun'
WHEN '7' THEN '07-Jul'
WHEN '8' THEN '08-Aug'
WHEN '9' THEN '09-Sept'
WHEN '10' THEN '10-Oct'
WHEN '11' THEN '11-Nov'
WHEN '12' THEN '12-Dec'
ELSE null
END )
as "Invoice Month2", 
MAX ('Q' || EXTRACT(QUARTER FROM QUERY."Invoice Date")) AS "Invoice Quarter", 
MAX (COALESCE(IMPORT."Hazard Flag", EXPO."Hazard Flag")) as "Hazard Flag", 
MAX (COALESCE(IMPORT."Origin Branch Region Code", EXPO."Origin Branch Region Code")) as "Origin Branch Region Code",
MAX (COALESCE(IMPORT."Destination Branch Region Code", EXPO."Destination Branch Region Code")) as "Destination Branch Region Code",
MAX(CONTAINERS."Container Count") as "Container Count",
MAX(CONTAINERS."Containers") AS "Containers",
MAX(CONTAINERS."Container Sizes") as "Container Sizes",
MAX(COALESCE(EXPO."QTY",IMPORT."QTY")) as "QTY",
MAX(COALESCE(EXPO."EN",IMPORT."EN")) as "EN Reference",
MAX(COALESCE(EXPO."CI",IMPORT."CI")) as "CI Reference",
MAX(COALESCE(EXPO."MB",IMPORT."MB")) AS "MB Reference",
MAX(COALESCE(EXPO."DP",IMPORT."DP")) AS "DP Reference"
FROM QUERY 
LEFT JOIN IMPORT ON QUERY."Internal Reference" = IMPORT."Invoice Number"
LEFT JOIN EXPO ON QUERY."Internal Reference" = EXPO."Invoice Number"
LEFT JOIN CONTAINERS ON COALESCE(EXPO."EI File Number",IMPORT."EI File Number") = CONTAINERS."EI File Number"
GROUP BY 
QUERY."Invoice Date Time",
QUERY."INVOICE EXPEDITOR", 
QUERY."INVOICE EXPEDITOR LINE", 
QUERY."Billing Code",
QUERY."Billing Line Description",
QUERY."Internal Reference",
QUERY."Invoice Date"
order by QUERY."Invoice Date Time",
QUERY."INVOICE EXPEDITOR", 
QUERY."INVOICE EXPEDITOR LINE", 
QUERY."Billing Code",
QUERY."Billing Line Description",
QUERY."Internal Reference",
QUERY."Invoice Date"