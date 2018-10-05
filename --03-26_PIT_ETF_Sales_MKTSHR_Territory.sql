---- THE FOLLOWING QUERY IS MEANT TO DRIVE MOST ETF REPORTING INCLUDING COMPENSATION -----
---- THE METHODOLOGY IS 4 FOLD:
----                         1) MATCH ACTUAL (ADVISOR BRANCH) DATA BY DEALER-REP CODE TO THE ADVISOR DATA IN SMDM TO FIND TERRITORIES
----                         2) MATCH ACTUAL (ADVISOR BRANCH) DATA BY 'BRANCH' TO THE ADVISOR DATA IN SMDM TO FIND TERRITORIES
----                         3) MATCH CORRELATED (CLIENT BRANCH) DATA TO THE CUSTOMER DATA IN SMDM TO FIND TERRITORIES
----                         4) USE SMDM DATA DIRECTLY FOR MANULIFE SECURITIES TO FIND TERRITORIES


--2018/03/07: Drop table IF exist
IF(object_id('tempdb..#tmp_ETF','u') > 0)
DROP TABLE #tmp_ETF;


CREATE TABLE #tmp_ETF (			-------- Create #temp table
 FirmName               VARCHAR(100)
,Comp_Territory         VARCHAR(100)
,Dealer_Type            VARCHAR(100)
,Branch                 VARCHAR(100)
,Office_Type            VARCHAR(100)
--,Account_Type           VARCHAR(25)
--,A_Territory            VARCHAR(25)
--,A_MF_Assets            VARCHAR(25)
--,C_BR                   VARCHAR(25)
--,C_Territory            VARCHAR(25)
--,C_MF_Assets            VARCHAR(25)
,Reporting_Date         DATETIME
,PEER_GROUP                  VARCHAR(100)
,Asset_Position_Shares  NUMERIC(15,2)
,Asset_Balance          NUMERIC(15,2)
--,Net_New_Assets         NUMERIC(15,2)
);

WITH cte1 AS (  -----------THIS SECTION PULLS ADVISOR BRANCHES FROM BROADRIDGE AND MAPS TO ADVISORS IN SMDM ----------------------
                SELECT
                   CD.Dealer_Code +'-'+aADDR.Postal_Code BR
                  ,TERRb.Territory_External_ID Territory
                 ,Row_Number() OVER (
                 Partition By (CD.Dealer_Code +'-'+aADDR.Postal_Code)
                      ORDER BY SUM(ASSET.Asset_Value_Amt) DESC, Count(Distinct DISTb.src_Party_ID) desc) rn
                 FROM 
                 DIS_BI_SALESMKT_RPT_P1.[dbo].ASSOCIATED_CODES_DIM CD
                 INNER JOIN 
                 DIS_BI_SALESMKT_RPT_P1.[dbo].SELLING_CODE_DIM SELL
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

                                                                
                                                GROUP BY
                                                                CD.Dealer_Code +'-'+aADDR.Postal_Code
                                                                ,TERRb.Territory_External_ID)

----- THIS IS THE MAIN PART OF THE QUERY --------
                INSERT INTO #tmp_ETF
                SELECT
                                etf.Firm_Name FirmName
							    ,CASE 
                                   WHEN ETF.MLI_Territory_Name is not null 
                                   THEN ETF.MLI_Territory_Name                               --THESE ARE THE TERRITORIES CONFIRMED ONLY VIA THE SALES TEAM, OUR PROCESS CANNOT FIND THE TERRITORY
                                   WHEN ETF.Dealer_Branch_CD in ('C72:DEFAULT','C57:DEFAULT','C58:DEFAULT','CAA:DEFAULT','C18:DEFAULT') -- THESE ARE HOUSE ACCOUNTS OR DISCOUNT BROKERAGES
                                   THEN 'Institutional'
                                   ELSE cte1.Territory
                                 END Comp_Territory
                                ,etf.Dealer_Type
                                ,etf.aBR Branch
                                ,etf.Office_Type
                                ,ETF.Reporting_Date Reporting_Date
                                ,ETF.PeerGroup PEER_GROUP
                                ,ETF.Asset_Position_Shares
                                ,ETF.Asset_Position_Balance Asset_Balance
                              
                FROM cte1
                
--- THIS SECTION IS ALL THE BROADRIDGE ETF DATA -----
                
                RIGHT JOIN (
                                                
										  SELECT
		     ASSET.Reporting_Date
			,FIRM.Firm_Name
			,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ BR.Office_Postal_CD aBR
--			,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD cBR
			,BR.Dealer_Branch_CD
			,BR.Office_Type
			,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END Dealer_Type
			,Asset.Rank_Group PeerGroup
			,SUM(ASSET.Industry_Shares_Amount) Asset_Position_Shares
			,SUM(ASSET.Industry_Asset_Amount) Asset_Position_Balance
			,T_REF.MLI_Territory_Name

			  FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_MARKET_SHARE_FACT] ASSET
			  INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_DEALER_BRANCH_FACT] BR
			  on ASSET.Dealer_Branch_ID = BR.Dealer_Branch_ID
			  INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_FIRM_FACT] FIRM
			  ON BR.Office_ID = FIRM.Firm_ID
			  INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_DEALER_FACT] DEALER
			  On FIRM.Firm_ID = Dealer.ETF_Dealer_Code
			  LEFT JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_TERRITORY_FACT] T_REF
			  ON BR.Dealer_Branch_CD = T_REF.ETF_Branch_Name
				AND (T_REF.Effective_Date<=ASSET.Reporting_Date
				AND T_REF.End_Date >=ASSET.Reporting_Date)

			  WHERE
			  (
			  Firm.Firm_Name <> 'MANULIFE SECURITIES'	--FILTERING SINCE WE CAN USE DEALER-REP CODE METHODOLOGY
			   AND
			  Firm.Firm_Name <> 'CIBC WORLD MARKETS CORP.'	--FILTERING SINCE WE CAN USE DEALER-REP CODE METHODOLOGY
			   AND
			  Firm.Firm_Name <> 'RBC DOMINION SECURITIES.'	--FILTERING SINCE WE CAN USE DEALER-REP CODE METHODOLOGY
			   AND
			  NOT (Firm.Firm_Name = 'SCOTIACAPITAL INC.' and BR.Dealer_Branch_CD not in ('C36:000'))) --FILTERING SINCE WE CAN USE DEALER-REP CODE METHODOLOGY -- but leaves the exception UMA type branches

------------------------------------------------------------------------------------------------
			 AND cast(ASSET.Reporting_Date as date) = (Select Max(cast(Reporting_Date as date)) FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].ETF_MARKET_SHARE_FACT ASSET) ------REMOVE ME!!!!!!!!
------------------------------------------------------------------------------------------------

GROUP BY
			  	 ASSET.Reporting_Date
				,FIRM.Firm_Name
				,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ BR.Office_Postal_CD
				,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END
				,Dealer.MLI_Dealer_Code
				,ASSET.Rank_Group
				,BR.Office_Type
				,BR.Dealer_Branch_CD
				,T_REF.MLI_Territory_Name
                ) ETF
                on cte1.br = ETF.aBR

                Where  (cte1.rn = 1 or cte1.rn is null);


with cte2 AS ( -----------THIS SECTION PULLS CLIENT BRANCHES FROM BROADRIDGE AND MAPS TO ADVISORS IN SMDM ----------------------

SELECT
                left(Host_Distributor_Num,4) + '-' + cADDR.Postal_Code cBranch
				,TERRb.Territory_External_ID Territory
				,SUM(ASSET.Asset_Value_Amt) Assets
				,Row_Number() OVER (
                         Partition By (left(Host_Distributor_Num,4) + '-' + cADDR.Postal_Code)
                         ORDER BY SUM(ASSET.Asset_Value_Amt) DESC ) rn

  FROM [DIS_BI_SALESMKT_RPT_P1].[dbo].[CUSTOMER_ADDRESS_DIM] cAddr
  INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].POLICY_CUSTOMER_FACT PC
  ON cAddr.Customer_ID = PC.Customer_ID
  INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].POLICY_DIM POL
  on PC.Policy_ID = pol.Policy_ID 
                INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].[SELLING_CODE_POLICY_FACT] PolSell
                ON POL.Policy_ID = PolSell.Policy_ID and PolSell.Active_Record_Ind = 'Y'
                INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].ASSET_DAILY_FACT ASSET
                on PolSell.Selling_Code_ID = ASSET.Reporting_Selling_Code_ID
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

Where 
   Customer_Role = 'owner'
   and
   (
   POL.Product_Name = 'Manulife Mutual Funds'
   )
      
   Group by
                 left(Host_Distributor_Num,4) + '-' + cADDR.Postal_Code
				 ,TERRb.Territory_External_ID
                )


INSERT INTO #tmp_ETF  ----- THIS IS THE MAIN PART OF THE QUERY --------
                SELECT
                                etf.Firm_Name FirmName
								,CASE 
                                                  WHEN ETF.MLI_Territory_Name is not null 
                                                  THEN ETF.MLI_Territory_Name                               --THESE ARE THE TERRITORIES CONFIRMED ONLY VIA THE SALES TEAM, OUR PROCESS CANNOT FIND THE TERRITORY
                                                  WHEN ETF.Dealer_Branch_CD in ('C72:DEFAULT','C57:DEFAULT','C58:DEFAULT','CAA:DEFAULT','C18:DEFAULT') -- THESE ARE HOUSE ACCOUNTS OR DISCOUNT BROKERAGES
                                                  THEN 'Institutional'
                                                  ELSE cte2.Territory
                                END Comp_Territory
                                ,etf.Dealer_Type
                                ,etf.aBR Branch
                                ,etf.Office_Type
                                ,ETF.Reporting_Date Reporting_Date
                                ,ETF.PeerGroup Peer_Group
                                ,ETF.Asset_Position_Shares
                                ,ETF.Asset_Position_Balance Asset_Balance
                              
                FROM cte2
                
--- THIS SECTION IS ALL THE BROADRIDGE ETF DATA -----
                
                RIGHT JOIN (
                                                
		SELECT
		     ASSET.Reporting_Date
			,FIRM.Firm_Name
			,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ BR.Office_Postal_CD aBR
			,BR.Dealer_Branch_CD
			,BR.Office_Type
			,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END Dealer_Type
			,Asset.Rank_Group PeerGroup
			,SUM(ASSET.Industry_Shares_Amount) Asset_Position_Shares
			,SUM(ASSET.Industry_Asset_Amount) Asset_Position_Balance
			,T_REF.MLI_Territory_Name

			  FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_MARKET_SHARE_FACT] ASSET
			  INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_DEALER_BRANCH_FACT] BR
			  on ASSET.Dealer_Branch_ID = BR.Dealer_Branch_ID
			  INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_FIRM_FACT] FIRM
			  ON BR.Office_ID = FIRM.Firm_ID
			  INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_DEALER_FACT] DEALER
			  On FIRM.Firm_ID = Dealer.ETF_Dealer_Code
			  LEFT JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_TERRITORY_FACT] T_REF
			  ON BR.Dealer_Branch_CD = T_REF.ETF_Branch_Name
				AND (T_REF.Effective_Date<=ASSET.Reporting_Date
				AND T_REF.End_Date >=ASSET.Reporting_Date)

			  WHERE
			  (
			  Firm.Firm_Name <> 'MANULIFE SECURITIES'		--FILTERING OUT MANULIFE SECURITTIES SINCE IT WILL BE ADDED FROM SMDM DIRECTLY!!!
			   AND
			  Firm.Firm_Name <> 'CIBC WORLD MARKETS CORP.'	--FILTERING SINCE WE CAN USE DEALER-REP CODE METHODOLOGY
			   AND
			  Firm.Firm_Name <> 'RBC DOMINION SECURITIES.'	--FILTERING SINCE WE CAN USE DEALER-REP CODE METHODOLOGY
			   AND
			  NOT (Firm.Firm_Name = 'SCOTIACAPITAL INC.' and BR.Dealer_Branch_CD not in ('C36:000'))) --FILTERING SINCE WE CAN USE DEALER-REP CODE METHODOLOGY -- but leaves the exception UMA type branches

------------------------------------------------------------------------------------------------
			 AND cast(ASSET.Reporting_Date as date) = (Select Max(cast(Reporting_Date as date)) FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].ETF_MARKET_SHARE_FACT ASSET) ------REMOVE ME!!!!!!!!
------------------------------------------------------------------------------------------------

GROUP BY
			  	 ASSET.Reporting_Date
				,FIRM.Firm_Name
				,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ BR.Office_Postal_CD
				,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END
				,Dealer.MLI_Dealer_Code
				,ASSET.Rank_Group
				,BR.Office_Type
				,BR.Dealer_Branch_CD
				,T_REF.MLI_Territory_Name
                ) ETF
                on cte2.cBranch = ETF.aBR

                Where   (cte2.rn = 1 or cte2.rn is null)


INSERT INTO #tmp_ETF ---- THIS SECTION PULLS THE BROARDRIGE ETF DATA AT THE DEALER-REP CODE LEVEL (FOR RBC, SCOTIA and CIBC)
SELECT  
								 ETF_ACTUAL.Firm_Name FirmName
                                ,SMDM.Territory_External_ID Comp_Territory
                                ,ETF_ACTUAL.Dealer_Type Dealer_Type 
                                ,Coalesce(SMDM.Selling_Code,ETF_ACTUAL.abr) Branch
                                ,ETF_ACTUAL.Office_Type Office_Type
                                --,ETF_ACTUAL.Account_Type Account_Type
                            --    ,SMDM.Territory_External_ID A_Territory
                            --    ,NULL A_MF_ASSETS
                                --,ETF_ACTUAL.cBR cBR
                            --    ,NULL C_Territory
                            --    ,NULL C_MF_ASSETS
                                ,ETF_ACTUAL.Reporting_Date Reporting_Date
                                ,ETF_ACTUAL.PEER_GROUP Peer_Group
                                ,ETF_ACTUAL.Asset_Position_Shares Shares
                                ,ETF_ACTUAL.Asset_Position_Balance Asset_Balance
                                --,ETF_ACTUAL.Asset_Position_Net_New_Assets Net_New_Assets
                                
FROM
                                (SELECT
                                     FIRM.Firm_Name
                                                ,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END Dealer_Type
                                                ,CAST(DEALER.MLI_Dealer_Code as varchar)+Right(BR.Dealer_Branch_CD,Len(BR.Dealer_Branch_CD)-4) aBR
                                                ,BR.Office_Type
                                                ,Cast(ASSET.Reporting_Date as Date) Reporting_Date
                                                --,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD cBR
                                                ,ASSET.Rank_Group PEER_GROUP
                                                ,SUM(ASSET.Industry_Shares_Amount) Asset_Position_Shares
                                                --,SUM(ASSET.Industry_Net_New_Assets) Asset_Position_Net_New_Assets
                                                ,SUM(ASSET.Industry_Asset_Amount) Asset_Position_Balance
                                               
                                FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].ETF_MARKET_SHARE_FACT ASSET
                                                  INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_DEALER_BRANCH_FACT] BR
                                                  on asset.Dealer_Branch_ID = BR.Dealer_Branch_ID
                                                  INNER JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_FIRM_FACT] FIRM
                                                  ON BR.Office_ID = FIRM.Firm_ID
                                                  LEFT JOIN [DIS_BI_BW_INTELLIGENCE_P1].[dbo].[ETF_ref_DEALER_FACT] DEALER
                                                  On FIRM.Firm_ID = Dealer.ETF_Dealer_Code

                                WHERE
------------------------------------------------------------------------------------------------
                                                cast(ASSET.Reporting_Date as date) = (Select Max(cast(Reporting_Date as date)) FROM [DIS_BI_BW_INTELLIGENCE_P1].[dbo].ETF_MARKET_SHARE_FACT ASSET) ------REMOVE ME!!!!!!!!
												AND
------------------------------------------------------------------------------------------------

                                                (
                                                DEALER.MLI_Dealer_Code in ('9190','9280')
                                                OR (DEALER.MLI_Dealer_Code = '9155' and br.Dealer_Branch_CD <> ('C36:000'))
                        )
                                                
                                                
                                GROUP BY
                                                 ASSET.Reporting_Date
                                                ,FIRM.Firm_Name
                                                ,CAST(DEALER.MLI_Dealer_Code as varchar)+Right(BR.Dealer_Branch_CD,Len(BR.Dealer_Branch_CD)-4)
                                                --,COALESCE(CAST(Dealer.MLI_Dealer_Code as varchar),CAST(FIRM.FIRM_ID as varchar)) +'-'+ acct.Account_Postal_CD
                                                ,CASE WHEN Dealer.MLI_Dealer_Code is null THEN 'INSTITUTIONAL' ELSE 'RETAIL' END
                                                ,asset.Rank_Group
                                                ,BR.Office_Type
                                                ,BR.Dealer_Branch_CD
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
--and CAST(code.Dealer_Code as varchar) + CAST(Code.Rep_Code as varchar) = '7584T062'

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
					-------------------- DELIBERATELY SELECTING CURRENT TERRITORU AND WHOLESALER ---------------
					,TERR_c.Territory_External_ID 
					--,COALESCE(WHOLE.FIRST_NAME + ' ','')+COALESCE(WHOLE.LAST_NAME + ' ','') WHOLESALER
	
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
					--LEFT JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].TERRITORY_WHOLESALER_FACT TW
					--ON TERR_c.Sales_Territory_ID = TW.Sales_Territory_ID and TW.KMC_Role_ID = 2
					--LEFT JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].KEY_MANULIFE_CONTACT_DIM WHOLE
					--ON TW.Key_Manulife_Contact_ID = WHOLE.Key_Manulife_Contact_ID

				--	WHERE SC.Selling_Code = '357085'

				GROUP BY
					 cast(SC.Selling_Code as varchar)+cast(DIST.src_Party_ID as varchar)
					,SC.Selling_Code
					,DIST.src_Party_ID
					,TERR_c.Territory_External_ID 
					--,COALESCE(WHOLE.FIRST_NAME + ' ','')+COALESCE(WHOLE.LAST_NAME + ' ','')
					)SC_PARTY
				ON	ASS_SC.Selling_Code = SC_PARTY.Selling_Code
					AND
					cast(SC_PARTY.Start_Date as Date) <= cast(ASS_SC.End_Date as date)
					AND
					cast(ASS_SC.Start_Date as date) <= cast(SC_PARTY.End_Date as date)
                                                                ) SMDM
                ON ETF_ACTUAL.aBR = SMDM.DlrRep
                
                AND (SMDM.THE_START <= ETF_ACTUAL.Reporting_Date                                                    --Can't make this PIT since sometimes we need DSS to create the SellingCode after the transaction.  This causes NULL values.
                AND SMDM.THE_END >= ETF_ACTUAL.Reporting_Date) 


create table #tmp_peer (
 CUSIP 			VARCHAR(25)		COLLATE SQL_Latin1_General_CP1_CI_AI
,PEER_GROUP		VARCHAR(50)		COLLATE SQL_Latin1_General_CP1_CI_AI
);

Insert into #tmp_peer
	VALUES 
('00850E100','US Equity Large Cap Blend'),
('05573X103','US Equity Large Cap Blend'),
('05581U109','US Equity Large Cap Blend'),
('05577D103','US Equity Large Cap Blend'),
('05577D202','US Equity Large Cap Blend'),
('05580L100','US Equity Large Cap Blend'),
('05584G107','US Equity Large Cap Value'),
('05584H105','US Equity Large Cap Blend'),
('05575X119','US Equity Large Cap Blend'),
('05575X101','US Equity Large Cap Blend'),
('05576C106','US Equity Large Cap Blend'),
('09661N100','US Equity Large Cap Blend'),
('05579F114','US Equity Large Cap Blend'),
('05579F106','US Equity Large Cap Blend'),
('10527K108','US Equity Large Cap Blend'),
('10527K116','US Equity Large Cap Blend'),
('11004A100','US Equity Large Cap Blend'),
('11004A209','US Equity Large Cap Blend'),
('25058Q100','US Equity Large Cap Blend'),
('26801J104','US Equity Large Cap Blend'),
('26802A102','US Equity Mid Cap'),
('30051L207','US Equity Large Cap Blend'),
('30051L108','US Equity Large Cap Blend'),
('31867C118','US Equity Large Cap Blend'),
('31867C100','US Equity Large Cap Blend'),
('31865E108','US Equity Large Cap Blend'),
('31865E306','US Equity Large Cap Blend'),
('31865E504','US Equity Large Cap Blend'),
('31864E109','US Equity Large Cap Blend'),
('31864E307','US Equity Large Cap Blend'),
('31864M119','US Equity Mid Cap'),
('31864M127','US Equity Mid Cap'),
('31864L111','US Equity Mid Cap'),
('31864L129','US Equity Mid Cap'),
('31862J100','US Equity Large Cap Blend'),
('31862J134','US Equity Large Cap Blend'),
('31867A104','US Equity Large Cap Blend'),
('31866J106','US Equity Large Cap Blend'),
('31866M117','US Equity Large Cap Blend'),
('35376P106','US Equity Large Cap Blend'),
('33740G100','US Equity Large Cap Blend'),
('33740H207','US Equity Large Cap Blend'),
('33740H108','US Equity Large Cap Blend'),
('44052M205','US Equity Large Cap Blend'),
('44052M205','US Equity Large Cap Blend'),
('44049W100','US Equity Large Cap Blend'),
('44049W100','US Equity Large Cap Blend'),
('46435T104','US Equity Large Cap Blend'),
('46435Q100','US Equity Large Cap Blend'),
('46433S108','US Equity Large Cap Blend'),
('46436R107','US Equity Large Cap Blend'),
('46435N107','US Equity Large Cap Blend'),
('46435R108','US Equity Large Cap Blend'),
('46433G104','US Equity Large Cap Blend'),
('46433B204','US Equity Large Cap Blend'),
('46433B105','US Equity Large Cap Blend'),
('46434Y104','US Equity Large Cap Blend'),
('46434B104','US Equity Large Cap Blend'),
('55453M107','US Equity Large Cap Blend'),
('56502J111','US Equity Large Cap Blend'),
('56502J103','US Equity Large Cap Blend'),
('56502K209','US Equity Large Cap Blend'),
('56502K100','US Equity Large Cap Blend'),
('56502T101','US Equity Mid Cap'),
('56502T200','US Equity Mid Cap'),
('73939F109','US Equity Large Cap Blend'),
('73939F208','US Equity Large Cap Blend'),
('73938K109','US Equity Large Cap Blend'),
('73939N201','US Equity Large Cap Blend'),
('73939N102','US Equity Large Cap Blend'),
('73939N300','US Equity Large Cap Blend'),
('73938N103','US Equity Large Cap Blend'),
('73938N202','US Equity Large Cap Blend'),
('73938N301','US Equity Large Cap Blend'),
('74640K106','US Equity Large Cap Blend'),
('74640K205','US Equity Large Cap Blend'),
('74933A104','US Equity Large Cap Blend'),
('74930L103','US Equity Large Cap Blend'),
('74930L202','US Equity Large Cap Blend'),
('74933E106','US Equity Large Cap Blend'),
('74933K110','US Equity Large Cap Blend'),
('74933K102','US Equity Large Cap Blend'),
('90291B112','US Equity Large Cap Blend'),
('90291B104','US Equity Large Cap Blend'),
('92206E108','US Equity Large Cap Blend'),
('92206F105','US Equity Large Cap Blend'),
('97718Q200','US Equity Large Cap Blend'),
('97718Q101','US Equity Large Cap Blend'),
('97719J205','US Equity Mid Cap'),
('97719J106','US Equity Mid Cap'),
('97718P111','US Equity Large Cap Blend'),
('97718P103','US Equity Large Cap Blend'),
('97719L101','US Equity Large Cap Blend')


	;

INSERT INTO #tmp_ETF --- THIS SECTION FINDS IS ALL THE MANULIFE SECURITIES ETF DATA FROM SMDM -----
SELECT
                'MANULIFE SECURITIES' FirmName
                ,TERR_c.Territory_External_ID Comp_Territory
                ,'RETAIL' Dealer_Type
                ,SCD2.Selling_Code Branch
                ,'SOURCE' Office_Type
                ,D.Calendar_date Reporting_Date
                ,HOLD.Peer_group Peer_Group
                ,SUM(HOLD.Units) Asset_Position_Shares
                ,SUM (HOLD.Assets) Assets_Balance

FROM 
                (
												SELECT 
                                                HOLD.Reporting_Date_ID,
                                                'H' Gross_Redemption_Type,
                                                Peer.PEER_GROUP,
                                                HOLD.Original_Selling_Code_ID,
                                                HOLD.Asset_Value_Amt Assets
                                                ,HOLD.Number_Of_Units Units
                                                                                 
                                                FROM DIS_BI_SALESMKT_RPT_P1.[dbo].[ASSET_DAILY_FACT] HOLD 
                                                INNER JOIN DIS_BI_SALESMKT_RPT_P1.dbo.DATE_DIM D 
                                                                ON HOLD.Reporting_Date_ID = D.Date_ID 
                                                INNER JOIN DIS_BI_SALESMKT_RPT_P1.[dbo].PRODUCT_DIM PROD
												ON HOLD.inv_Product_ID = PROD.Product_ID
												Inner Join #tmp_peer PEER 
                                                ON PROD.Product_Name = PEER.CUSIP 
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
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].DISTRIBUTOR_SALES_TERRITORY_FACT DT
	ON DD3.Distributor_ID = DT.Distributor_ID AND DT.Territory_Type_ID = 4 AND DT.Active_Record_Ind = 'Y'
	INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SALES_TERRITORY_DIM TERR
	ON DT.Sales_Territory_ID = TERR.Sales_Territory_ID
		INNER JOIN [DIS_BI_SALESMKT_RPT_P1].[dbo].SALES_TERRITORY_DIM TERR_c
		ON TERR.src_ID = TERR_c.src_ID and TERR_c.Active_Record_Ind = 'Y'
               
GROUP BY 
                TERR_c.Territory_External_ID,
                D.Calendar_date
                ,SCD2.Selling_Code
                ,HOLD.Peer_group


SELECT DISTINCT * FROM  #tmp_ETF

--2018/03/07: cleanup the table
IF(object_id('tempdb..#tmp_ETF','u') > 0)
DROP TABLE #tmp_ETF;
DROP TABLE #tmp_peer;