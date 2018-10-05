---- THE FOLLOWING QUERY IS MEANT TO DRIVE MOST ETF REPORTING INCLUDING COMPENSATION -----
---- THE METHODOLOGY IS 5 FOLD:
----                         1) MATCH ACTUAL (ADVISOR BRANCH) DATA BY DEALER-REP CODE TO THE ADVISOR DATA IN SMDM TO FIND TERRITORIES
----                         2) MATCH ACTUAL (ADVISOR BRANCH) DATA BY 'BRANCH' TO THE ADVISOR DATA IN SMDM TO FIND TERRITORIES
----                         3) MATCH CORRELATED (CLIENT BRANCH) DATA TO THE CUSTOMER DATA IN SMDM TO FIND TERRITORIES
----                         4) USE SMDM DATA DIRECTLY FOR MANULIFE SECURITIES TO FIND TERRITORIES
----                         5) USE DATA PROVIDED DIRECTLY FROM THE DEALERS THAT SEND IT TO US (i.e. TD Waterhouse)


--2018/03/07: Drop table IF exist
IF(object_id('tempdb..#tmp_ETF','u') > 0)
DROP TABLE #tmp_ETF;

--2018/03/07:Create #temp table
CREATE TABLE #tmp_ETF (
 FirmName               VARCHAR(50)
,Comp_Territory         VARCHAR(25)
,Dealer_Type            VARCHAR(25)
,Branch                 VARCHAR(25)
,Office_Type            VARCHAR(25)
,Account_Type           VARCHAR(25)
,C_BR                   VARCHAR(25)
,Reporting_Date         DATETIME
,CUSIP                  VARCHAR(25)
,Asset_Position_Shares  NUMERIC(15,6)
,Asset_Balance          NUMERIC(15,2)
,Net_New_Assets         NUMERIC(15,2)
);

-----------THIS SECTION PULLS ADVISOR BRANCHES FROM BROADRIDGE AND MAPS TO ADVISORS IN SMDM ----------------------

WITH cte1 AS (SELECT TOP 100 PERCENT
                   CD.Dealer_Code +'-'+REPLACE(aADDR.Postal_Code,' ','-') BR
                  ,TERRb.Territory_External_ID Territory
                 ,Row_Number() OVER (
                 Partition By (CD.Dealer_Code +'-'+REPLACE(aADDR.Postal_Code,' ','-'))
                      ORDER BY SUM(ASSET.Asset_Value_Amt) DESC, Count(Distinct DISTb.src_Party_ID) desc) rn
                 
				 FROM DIS_BI_SALESMKT_RPT_P1.[dbo].ASSOCIATED_CODES_DIM CD
                 INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].SELLING_CODE_DIM SELL
                 ON CD.Selling_Code_ID = SELL.Selling_Code_ID
                    INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].SELLING_CODE_DIM SELLb
                    ON SELL.Selling_Code = SELLb.Selling_Code and SELLb.Active_Record_Ind = 'Y'
                                                                
                 LEFT JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].ASSET_DAILY_FACT ASSET
                 ON CD.Associated_Codes_ID = ASSET.Original_Associated_Codes_ID

                 INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].DISTRIBUTOR_DIM DIST
                 ON SELLb.Distributor_ID = DIST.Distributor_ID
                          INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].DISTRIBUTOR_DIM DISTb
                          ON DIST.src_Party_ID = DISTb.src_Party_ID and DISTb.Active_Record_Ind = 'Y'

                 INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].DISTRIBUTOR_SALES_TERRITORY_FACT sTERR
                 ON DISTb.Distributor_ID = sTERR.Distributor_ID and sTERR.Territory_Type_ID = 4 and sTerr.Active_Record_Ind = 'Y'
                 INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].SALES_TERRITORY_DIM TERR
                 ON sTERR.Sales_Territory_ID = TERR.Sales_Territory_ID
                          INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].SALES_TERRITORY_DIM TERRb
                          ON TERR.src_ID = TERRb.src_ID and TERRb.Active_Record_Ind ='Y'

                 INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].DISTRIBUTOR_ADDRESS_DIM aADDR
                 ON DISTb.src_Party_ID = aADDR.src_Party_ID and aADDR.Active_Record_Ind = 'Y'

	        Where   
					TERRb.Territory_Name not in ('TERM','TBQ','Non Wealth')
                                                                
			GROUP BY
				CD.Dealer_Code +'-'+REPLACE(aADDR.Postal_Code,' ','-')
				,TERRb.Territory_External_ID

			ORDER BY
				CD.Dealer_Code +'-'+REPLACE(aADDR.Postal_Code,' ','-')
			)


----- THIS IS THE MAIN PART OF THE QUERY --------
               --2018/03/07: INSERT 
INSERT INTO #tmp_ETF
SELECT   etf.Firm_Name
		,CASE WHEN ETF.MLI_Territory_Name is not null THEN ETF.MLI_Territory_Name    --THESE ARE THE TERRITORIES CONFIRMED ONLY VIA THE SALES TEAM, OUR PROCESS CANNOT FIND THE TERRITORY
        ELSE cte1.Territory
        END Comp_Territory
        ,etf.Dealer_Type
        ,etf.aBR
        ,'ACTUAL' Office_Type
        ,etf.Account_Type
        ,etf.cBR
        ,ETF.Asset_Position_Reporting_Date Reporting_Date
        ,ETF.CUSIP
        ,ETF.Asset_Position_Shares
        ,ETF.Asset_Position_Balance Asset_Balance
        ,ETF.Asset_Position_Net_New_Assets Net_New_Assets
                              
FROM cte1      
--- THIS SECTION IS ALL THE BROADRIDGE ETF DATA -----
RIGHT JOIN (SELECT
				 Eomonth(ASSET.Asset_Position_Reporting_Date) Asset_Position_Reporting_Date
                ,FIRM.Firm_Name
                ,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ BR.Office_Postal_CD aBR
                ,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD cBR
                ,BR.Dealer_Branch_CD
                ,BR.Office_Type
                ,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END Dealer_Type
                ,acct.Account_Type_TA_CD Account_Type
                ,PROD.Product_CUSIP CUSIP
                ,SUM(ASSET.Asset_Position_Shares) Asset_Position_Shares
                ,SUM(ASSET.Asset_Position_Net_New_Assets) Asset_Position_Net_New_Assets
                ,SUM(ASSET.Asset_Position_Balance) Asset_Position_Balance
                ,T_REF.MLI_Territory_Name

			FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ACCOUNT_FACT] acct
                INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_HOLDINGS_DIM] hold
                on acct.Account_ID = hold.Account_ID
                INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ASSET_POSITION_FACT] ASSET
                ON Hold.Holdings_ID = ASSET.Asset_Position_Holding_ID
                INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_DEALER_BRANCH_FACT] BR
                on hold.Dealer_Branch_ID = BR.Dealer_Branch_ID
                INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_FIRM_FACT] FIRM
                ON BR.Office_ID = FIRM.Firm_ID
                LEFT JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_DEALER_FACT] DEALER
                On FIRM.Firm_ID = Dealer.ETF_Dealer_Code
                INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_PRODUCT_FACT] PROD
                ON hold.Product_ID = PROD.Product_ID
                LEFT JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_TERRITORY_FACT] T_REF
                ON BR.Dealer_Branch_CD = T_REF.ETF_Branch_Name
                        AND (T_REF.Effective_Date<=ASSET.Asset_Position_Reporting_Date
                        AND T_REF.End_Date >=ASSET.Asset_Position_Reporting_Date)

            WHERE NOT (BR.Office_Type = 'Correlated' 
				OR BR.Dealer_Branch_CD in ('C36:000','C36:013')
				OR RIGHT(BR.Dealer_Branch_CD,7) = 'DEFAULT') -- THESE ARE EXCEPTION 'ACTUAL' BRANCHES THAT NEED TO BE TREATED LIKE CORRELATED (Like UMA).
				AND
				DEALER.MLI_Dealer_Code not in ('9280','9190','9155')	        --FILTERING CIBC & RBC SINCE WE CAN USE DEALER-REP CODE METHODOLOGY

			GROUP BY
				Eomonth(ASSET.Asset_Position_Reporting_Date)
				,FIRM.Firm_Name
				,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ BR.Office_Postal_CD
				,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD
				,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END
				,PROD.Product_CUSIP
				,BR.Office_Type
				,BR.Dealer_Branch_CD
				,acct.Account_Type_TA_CD
				,T_REF.MLI_Territory_Name
			) ETF
on cte1.br = ETF.aBR

Where  (cte1.rn = 1 or cte1.rn is null);

-----------THIS SECTION PULLS CLIENT BRANCHES FROM BROADRIDGE AND MAPS TO ADVISORS IN SMDM ----------------------

with cte2 AS (SELECT TOP 100 PERCENT
					left(Host_Distributor_Num,4) + '-' + REPLACE(cADDR.Postal_Code,' ','-') cBranch 
					,TERRb.Territory_External_ID Territory
					,SUM(ASSET.Asset_Value_Amt) Assets
					,Row_Number() OVER (
								Partition By (left(Host_Distributor_Num,4) + '-' + REPLACE(cADDR.Postal_Code,' ','-') )
								ORDER BY SUM(ASSET.Asset_Value_Amt) DESC ) rn

			FROM [DIS_BI_SALESMKT_RPT_P1].[dbo].[CUSTOMER_ADDRESS_DIM] cAddr
					INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].POLICY_CUSTOMER_FACT PC
					ON cAddr.Customer_ID = PC.Customer_ID
					INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].POLICY_DIM POL
					on PC.Policy_ID = pol.Policy_ID 
					INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].[SELLING_CODE_POLICY_FACT] PolSell
					ON POL.Policy_ID = PolSell.Policy_ID and PolSell.Active_Record_Ind = 'Y'

					LEFT JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].ASSET_DAILY_FACT ASSET
					on Pol.Policy_ID = ASSET.Policy_ID

					INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].SELLING_CODE_DIM SELL
					ON POLSell.Selling_Code_ID = SELL.Selling_Code_ID
							INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].SELLING_CODE_DIM SELLb
							ON SELL.Selling_Code = SELLb.Selling_Code and SELLb.Active_Record_Ind = 'Y'
					INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].DISTRIBUTOR_DIM DIST
					ON SELLb.Distributor_ID = DIST.Distributor_ID
							INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].DISTRIBUTOR_DIM DISTb
							ON DIST.src_Party_ID = DISTb.src_Party_ID and DISTb.Active_Record_Ind = 'Y'

					INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].DISTRIBUTOR_SALES_TERRITORY_FACT sTERR
					ON DISTb.Distributor_ID = sTERR.Distributor_ID and sTERR.Territory_Type_ID = 4 and sTerr.Active_Record_Ind = 'Y'
					INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].SALES_TERRITORY_DIM TERR
					ON sTERR.Sales_Territory_ID = TERR.Sales_Territory_ID
							INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].SALES_TERRITORY_DIM TERRb
							ON TERR.src_ID = TERRb.src_ID and TERRb.Active_Record_Ind ='Y'

			Where Customer_Role = 'owner'
				and
				POL.Product_Name = 'Manulife Mutual Funds'
      
			Group by 
				left(Host_Distributor_Num,4) + '-' + REPLACE(cADDR.Postal_Code,' ','-') 
				,TERRb.Territory_External_ID

			ORDER BY
				left(Host_Distributor_Num,4) + '-' + REPLACE(cADDR.Postal_Code,' ','-')
						)

----- THIS IS THE MAIN PART OF THE QUERY --------
               --2018/03/07: INSERT 
INSERT INTO #tmp_ETF
SELECT
    etf.Firm_Name
	,CASE WHEN ETF.MLI_Territory_Name is not null THEN ETF.MLI_Territory_Name --THESE ARE THE TERRITORIES CONFIRMED ONLY VIA THE SALES TEAM, OUR PROCESS CANNOT FIND THE TERRITORY
    ELSE cte2.Territory
    END Comp_Territory
    ,etf.Dealer_Type
    ,etf.aBR
    ,'CORRELATED' Office_Type
    ,etf.Account_Type
    ,etf.cBR
    ,ETF.Asset_Position_Reporting_Date Reporting_Date
    ,ETF.CUSIP
    ,ETF.Asset_Position_Shares
    ,ETF.Asset_Position_Balance Asset_Balance
    ,ETF.Asset_Position_Net_New_Assets Net_New_Assets
                              
FROM cte2
                
--- THIS SECTION IS ALL THE BROADRIDGE ETF DATA -----
RIGHT JOIN (SELECT
				Eomonth(ASSET.Asset_Position_Reporting_Date) Asset_Position_Reporting_Date
				,FIRM.Firm_Name
				,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ BR.Office_Postal_CD aBR
				,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD cBR
				,BR.Dealer_Branch_CD
				,BR.Office_Type
				,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END Dealer_Type
				,acct.Account_Type_TA_CD Account_Type
				,PROD.Product_CUSIP CUSIP
				,SUM(ASSET.Asset_Position_Shares) Asset_Position_Shares
				,SUM(ASSET.Asset_Position_Net_New_Assets) Asset_Position_Net_New_Assets
				,SUM(ASSET.Asset_Position_Balance) Asset_Position_Balance
				,T_REF.MLI_Territory_Name

			FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ACCOUNT_FACT] acct
				INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_HOLDINGS_DIM] hold
				on acct.Account_ID = hold.Account_ID
				INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ASSET_POSITION_FACT] ASSET
				ON Hold.Holdings_ID = ASSET.Asset_Position_Holding_ID
				INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_DEALER_BRANCH_FACT] BR
				on hold.Dealer_Branch_ID = BR.Dealer_Branch_ID
				INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_FIRM_FACT] FIRM
				ON BR.Office_ID = FIRM.Firm_ID
				LEFT JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_DEALER_FACT] DEALER
				On FIRM.Firm_ID = Dealer.ETF_Dealer_Code
				INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_PRODUCT_FACT] PROD
				ON hold.Product_ID = PROD.Product_ID
				LEFT JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_TERRITORY_FACT] T_REF
				ON BR.Dealer_Branch_CD = T_REF.ETF_Branch_Name
					AND (T_REF.Effective_Date<=ASSET.Asset_Position_Reporting_Date
					AND T_REF.End_Date >=ASSET.Asset_Position_Reporting_Date)

			WHERE
				(BR.Office_Type = 'Correlated' 
				OR BR.Dealer_Branch_CD in ('C36:000','C36:013') -- THESE ARE EXCEPTION 'ACTUAL' BRANCHES THAT NEED TO BE TREATED LIKE CORRELATED (Like UMA)
				OR RIGHT(BR.Dealer_Branch_CD,7) = 'DEFAULT') -- THESE "DEFAULT" BRANCHES SHOULD BE ASSIGNED LIKE CORRELATED BRANCHES (as per Broadridge)
				AND	
				DEALER.MLI_Dealer_Code not in ('9834','7585','9280','9190')	  --Filtering TD, MSec, CIBC, RBC, Scotia	 								 
				AND
				NOT (DEALER.MLI_Dealer_Code = '9155' and Dealer_Branch_CD not in ('C36:000','C36:013')) --FILTERING SINCE WE CAN USE DEALER-REP CODE METHODOLOGY -- but leaves the exception UMA type branches


			GROUP BY
				Eomonth(ASSET.Asset_Position_Reporting_Date)
				,FIRM.Firm_Name
				,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ BR.Office_Postal_CD
				,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD
				,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END
				,PROD.Product_CUSIP
				,BR.Office_Type
				,BR.Dealer_Branch_CD
				,acct.Account_Type_TA_CD
				,T_REF.MLI_Territory_Name
          ) ETF
on cte2.cBranch = ETF.cBR

Where   (cte2.rn = 1 or cte2.rn is null)

---- THIS SECTION PULLS THE BROARDRIGE ETF DATA AT THE DEALER-REP CODE LEVEL (FOR RBC, SCOTIA and CIBC)
--2018/03/07: UNION 
INSERT INTO #tmp_ETF
SELECT  ETF_ACTUAL.Firm_Name Firm_Name
        ,SMDM.Territory_External_ID Comp_Territory
        ,ETF_ACTUAL.Dealer_Type Dealer_Type 
        ,Coalesce(SMDM.Selling_Code,ETF_ACTUAL.abr) aBR1
        ,ETF_ACTUAL.Office_Type Office_Type
        ,ETF_ACTUAL.Account_Type Account_Type
        ,ETF_ACTUAL.cBR cBR
        ,ETF_ACTUAL.Reporting_Date Reporting_Date
        ,ETF_ACTUAL.CUSIP CUSIP
        ,ETF_ACTUAL.Asset_Position_Shares Shares
        ,ETF_ACTUAL.Asset_Position_Balance Asset_Balance
        ,ETF_ACTUAL.Asset_Position_Net_New_Assets Net_New_Assets
                                
FROM
	(SELECT
            FIRM.Firm_Name
			,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END Dealer_Type
			,CAST(DEALER.MLI_Dealer_Code as varchar)+Right(BR.Dealer_Branch_CD,Len(BR.Dealer_Branch_CD)-4) aBR
			,BR.Office_Type
			,Cast(Eomonth(ASSET.Asset_Position_Reporting_Date) as Date) Reporting_Date
			,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD cBR
			,acct.Account_Type_TA_CD Account_Type
			,PROD.Product_CUSIP CUSIP
			,SUM(ASSET.Asset_Position_Shares) Asset_Position_Shares
			,SUM(ASSET.Asset_Position_Net_New_Assets) Asset_Position_Net_New_Assets
            ,SUM(ASSET.Asset_Position_Balance) Asset_Position_Balance
                                               
    FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ACCOUNT_FACT] acct
            INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_HOLDINGS_DIM] hold
            on acct.Account_ID = hold.Account_ID
            INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ASSET_POSITION_FACT] ASSET
            ON Hold.Holdings_ID = ASSET.Asset_Position_Holding_ID
            INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_DEALER_BRANCH_FACT] BR
            on hold.Dealer_Branch_ID = BR.Dealer_Branch_ID
            INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_FIRM_FACT] FIRM
            ON BR.Office_ID = FIRM.Firm_ID
            LEFT JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_DEALER_FACT] DEALER
            On FIRM.Firm_ID = Dealer.ETF_Dealer_Code
            INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_PRODUCT_FACT] PROD
            ON hold.Product_ID = PROD.Product_ID

    WHERE
			DEALER.MLI_Dealer_Code in ('9190','9280','9155')
			AND br.Dealer_Branch_CD not in ('C36:000','C36:013')
			AND RIGHT(BR.Dealer_Branch_CD,7) <> 'DEFAULT'
                                     
    GROUP BY
            Cast(Eomonth(ASSET.Asset_Position_Reporting_Date) as Date)
			,FIRM.Firm_Name
			,CAST(DEALER.MLI_Dealer_Code as varchar)+Right(BR.Dealer_Branch_CD,Len(BR.Dealer_Branch_CD)-4)
			,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD
			,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END
			,PROD.Product_CUSIP
			,BR.Office_Type
			,acct.Account_Type_TA_CD
	) ETF_ACTUAL

LEFT JOIN (	
			SELECT	 
				ASS_SC.DlrRep
				,ASS_SC.Selling_Code
				,SC_PARTY.src_Party_ID
				,SC_PARTY.Territory_External_ID 
				--,SC_PARTY.Wholesaler
				,case when ASS_SC.Start_Date <= SC_PARTY.Start_Date
					THEN SC_PARTY.Start_Date
					ELSE ASS_SC.start_date
					END as THE_START
				,case when ASS_SC.end_Date <= SC_PARTY.end_Date
					THEN ASS_SC.End_Date
					ELSE SC_PARTY.End_Date	
					END as THE_END	
	
			FROM
				(SELECT
					CAST(code.Dealer_Code as varchar) + CAST(Code.Rep_Code as varchar) +SC.Selling_Code ASS_SC_ID
					,CAST(code.Dealer_Code as varchar) + CAST(Code.Rep_Code as varchar) DlrRep
					,SC.Selling_Code
					,cast(Min(SC.Rec_Created_Date) as date) Start_Date
					,cast(Max(SC.Rec_Term_Date) as date) End_Date

				FROM [DIS_BI_SALESMKT_RPT_P1].[dbo].ASSOCIATED_CODES_DIM CODE
					INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SELLING_CODE_DIM SC
					ON CODE.Selling_Code_ID = SC.Selling_Code_ID

				Where Code.Dealer_Code is not null

				GROUP By
					CAST(code.Dealer_Code as varchar) + CAST(Code.Rep_Code as varchar) +SC.Selling_Code
					,CAST(code.Dealer_Code as varchar) + CAST(Code.Rep_Code as varchar)
					,SC.Selling_Code
				)ASS_SC

			INNER JOIN
				(SELECT
					 cast(SC.Selling_Code as varchar)+cast(DIST.src_Party_ID as varchar) SC_Party_ID
					,SC.Selling_Code
					,DIST.src_Party_ID
					-------------------- DELIBERATELY SELECTING CURRENT TERRITORY ---------------
					,TERR_c.Territory_External_ID 
					,cast(Min(SC.Rec_Created_Date) as date) Start_Date
					,cast(Max(SC.Rec_Term_Date) as date) End_Date

				FROM [DIS_BI_SALESMKT_RPT_P1].[dbo].SELLING_CODE_DIM SC
					INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].DISTRIBUTOR_DIM DIST
					ON SC.Distributor_ID = DIST.Distributor_ID
					INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].DISTRIBUTOR_DIM DIST_c
					ON DIST.src_Party_ID = DIST_c.src_Party_ID and DIST_C.Active_Record_Ind = 'Y'
					INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].DISTRIBUTOR_SALES_TERRITORY_FACT DT
					ON DIST_c.Distributor_ID = DT.Distributor_ID AND DT.Territory_Type_ID = 4 AND DT.Active_Record_Ind = 'Y'
					INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SALES_TERRITORY_DIM TERR
					ON DT.Sales_Territory_ID = TERR.Sales_Territory_ID
						INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SALES_TERRITORY_DIM TERR_c
						ON TERR.src_ID = TERR_c.src_ID and TERR_c.Active_Record_Ind = 'Y'


				GROUP BY
					 cast(SC.Selling_Code as varchar)+cast(DIST.src_Party_ID as varchar)
					,SC.Selling_Code
					,DIST.src_Party_ID
					,TERR_c.Territory_External_ID 
				)SC_PARTY
			ON	ASS_SC.Selling_Code = SC_PARTY.Selling_Code
					AND
					cast(SC_PARTY.Start_Date as Date) <= cast(ASS_SC.End_Date as date)
					AND
					cast(ASS_SC.Start_Date as date) <= cast(SC_PARTY.End_Date as date)
       ) SMDM
ON ETF_ACTUAL.aBR = SMDM.DlrRep
AND (SMDM.THE_START <= ETF_ACTUAL.Reporting_Date                --Can't make this PIT since sometimes we need DSS to create the SellingCode after the transaction.  This causes NULL values.
AND SMDM.THE_END >= ETF_ACTUAL.Reporting_Date)

--- THIS SECTION FINDS IS ALL THE MANULIFE SECURITIES ETF DATA FROM SMDM -----

--2018/03/07: UNION 
INSERT INTO #tmp_ETF
SELECT
        'MANULIFE SECURITIES' Firm_Name
        ,TERR_c.Territory_External_ID Comp_Territory
        ,'RETAIL' Dealer_Type
        ,SCD2.Selling_Code aBR
        ,'SOURCE' Office_Type
        ,'1' Account_Type
        ,NULL cBR
        ,EOMONTH(D.Calendar_date) Reporting_Date
        ,PROD.Product_Name CUSIP
        ,SUM(HOLD.Units) Asset_Position_Shares
        ,SUM (Case When HOLD.Gross_Redemption_Type='H' then (HOLD.Txn_amt) else 0 end) Assets_Balance
        ,(SUM (Case When HOLD.Gross_Redemption_Type='G' then (HOLD.Txn_amt) else 0 end))+((SUM (Case When HOLD.Gross_Redemption_Type='R' then (HOLD.Txn_amt) else 0 end))) Net_New_Assets

FROM 
	(SELECT    --THE FOLLOWING SUB-QUERY IS A UNION OF SMDM TRANSACTIONS AND ASSETS --
		NBV.Reporting_Date_ID,
		NBV.Gross_Redemption_Type,
		NBV.Product_ID inv_Product_ID,
		NBV.Original_Selling_Code_ID,
		NBV.Txn_Amt
		,NUll Units
                                                                
	FROM DIS_BI_SALESMKT_RPT_P1.dbo.NBV_MONTHEND_FACT NBV 
		INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.DATE_DIM D 
		ON NBV.Reporting_Date_ID = D.Date_ID 
		Inner Join DIS_BI_SALESMKT_RPT_P1.dbo.PRODUCT_DIM PROD 
		ON NBV.Product_ID = Prod.Product_ID 
		Where 
						PROD.Product_Name IN (SELECT Product_CUSIP FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_PRODUCT_FACT] WHERE Product_Type = 'ETF')
						and NBV.Gross_Redemption_Type in ('G','R')

	UNION SELECT 
		HOLD.Reporting_Date_ID,
		'H',
		HOLD.inv_Product_ID,
		HOLD.Original_Selling_Code_ID,
		HOLD.Asset_Value_Amt
		,HOLD.Number_Of_Units
                                                                                
	FROM DIS_BI_SALESMKT_RPT_P1.[dbo].[ASSET_MONTHEND_FACT] HOLD 
		INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.DATE_DIM D 
		ON HOLD.Reporting_Date_ID = D.Date_ID 
		Inner Join DIS_BI_SALESMKT_RPT_P1.dbo.PRODUCT_DIM PROD 
		ON HOLD.inv_Product_ID = Prod.Product_ID 
	WHERE PROD.Product_Name IN (SELECT Product_CUSIP FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_PRODUCT_FACT] WHERE Product_Type = 'ETF')
    ) HOLD 
	INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.DATE_DIM D 
	ON HOLD.Reporting_Date_ID = D.Date_ID 
	INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.SELLING_CODE_DIM SCD1
	ON HOLD.Original_Selling_Code_ID = SCD1.Selling_Code_ID
		INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.[SELLING_CODE_DIM] SCD2 
		ON SCD1.Selling_Code = SCD2.Selling_Code
			AND (Cast(SCD2.Rec_Created_Date as date)<= Cast(D.Calendar_Date as Date)
			AND Cast(SCD2.Rec_Term_Date as Date) >= Cast(D.Calendar_Date as Date))
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].DISTRIBUTOR_DIM DD1
	ON SCD2.Distributor_ID = DD1.Distributor_ID
		INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.[DISTRIBUTOR_DIM] DD2 
		ON DD2.src_Party_ID=DD1.src_Party_ID
			AND ISNULL(DD2.Branch_Code,1) = 1
			AND (CAST(DD2.Rec_Created_Date as Date) <= Cast(D.Calendar_Date as Date)
			AND CAST(DD2.Rec_Term_Date as Date) >= Cast(D.Calendar_Date as Date))
		INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.[DISTRIBUTOR_DIM] DD3
		ON DD2.src_Party_ID = DD3.src_Party_ID AND DD3.Active_Record_Ind = 'Y'
	--LEFT JOIN DIS_BI_SALESMKT_RPT_P1.dbo.DISTRIBUTOR_ADDRESS_DIM DDA
	--ON DD2.src_Party_ID = DDA.src_Party_ID and DDA.Active_Record_Ind = 'Y'

	Inner Join DIS_BI_SALESMKT_RPT_P1.dbo.PRODUCT_DIM PROD 
	on HOLD.inv_Product_ID = Prod.Product_ID
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].DISTRIBUTOR_SALES_TERRITORY_FACT DT
	ON DD3.Distributor_ID = DT.Distributor_ID AND DT.Territory_Type_ID = 4 AND DT.Active_Record_Ind = 'Y'
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SALES_TERRITORY_DIM TERR
	ON DT.Sales_Territory_ID = TERR.Sales_Territory_ID
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SALES_TERRITORY_DIM TERR_c
	ON TERR.src_ID = TERR_c.src_ID and TERR_c.Active_Record_Ind = 'Y'
                          
Where
     PROD.Active_Record_Ind = 'Y'
               
GROUP BY 
     TERR_c.Territory_External_ID
    ,EOMONTH(D.Calendar_date)
    ,SCD2.Selling_Code
    ,PROD.Product_Name

-- This Section pulls all the Source Data we get from Dealers
----- Currently, we only get data directly from TD Waterhouse of Assets.  
----- There is an Access Database that stores the raw data from them and converts to the desired format.

--2018/03/07: UNION
INSERT INTO #tmp_ETF
SELECT
		'TD WATERHOUSE' Firm_Name
		,TERR_c.Territory_External_ID Comp_Territory
		,'RETAIL' Dealer_Type
		,Source.Selling_Code aBR
		,'SOURCE' Office_Type
		,'1' Account_Type
		,NULL cBR
		,Eomonth(Source.Reporting_Date) Reporting_Date
		,Source.CUSIP CUSIP
		,SUM(Source.Units) Asset_Position_Shares
		,SUM(Source.Assets) Assets_Balance
		,SUM(Source.Net_Sales) Net_New_Assets
FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_DIRECT_SOURCE] as Source
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].[SELLING_CODE_DIM] SC1
	ON Source.Selling_Code = SC1.Selling_Code and SC1.Active_Record_Ind = 'Y'  --Can't make this PIT since sometimes we need DSS to create the SellingCode after the transaction.  This causes NULL values.
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].DISTRIBUTOR_DIM DD1
	ON SC1.Distributor_ID = DD1.Distributor_ID
        INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.[DISTRIBUTOR_DIM] DD2 
		ON DD2.src_Party_ID=DD1.src_Party_ID 
			AND DD2.Active_Record_Ind='Y'
			AND ISNULL(DD2.Branch_Code,1) = 1
	LEFT JOIN DIS_BI_SALESMKT_RPT_P1.dbo.[DISTRIBUTOR_SALES_TERRITORY_FACT] DSTF1 
	ON DD2.Distributor_ID = DSTF1.Distributor_ID AND DSTF1.Active_Record_Ind='Y' AND DSTF1.Territory_Type_ID = 4 
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].DISTRIBUTOR_SALES_TERRITORY_FACT DT
	ON DD2.Distributor_ID = DT.Distributor_ID AND DT.Territory_Type_ID = 4 AND DT.Active_Record_Ind = 'Y'
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SALES_TERRITORY_DIM TERR
	ON DT.Sales_Territory_ID = TERR.Sales_Territory_ID
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SALES_TERRITORY_DIM TERR_c
	ON TERR.src_ID = TERR_c.src_ID and TERR_c.Active_Record_Ind = 'Y'


GROUP BY
		TERR_c.Territory_External_ID
		,Source.Selling_Code
		,Eomonth(Source.Reporting_Date)
		,Source.CUSIP

--2018/03/07: retrieve DISTINCT results (non duplicates)

SELECT DISTINCT * FROM  #tmp_ETF

--2018/03/07: cleanup the table
IF(object_id('tempdb..#tmp_ETF','u') > 0)
DROP TABLE #tmp_ETF;