-- Comment selected lines: Ctrl + K, then Ctrl + C
-- Uncomment selected lines: Ctrl + K, then Ctrl + U

SELECT TOP 150 *
-- FROM UDIC_PEPQCCheck TABLE
	FROM [dbo].[UDIC_PEPQCCheck]
	WHERE UDIC_UID = '3ba9fbba870542b88d05adc7b5eb2ba5';
	--WHERE 
-- FROM CONTACT TABLE
  --FROM [Vantagepoint].[dbo].[Contacts]
  --WHERE ContactID IN ('7D49DB903C19449EBE4F7AC9CDF021AF', '16AB2B7C875A4667A62FCDC44079E99A');
-- FROM CLIENT TABLE
  --FROM  [Vantagepoint].[dbo].[CL]
  --WHERE ContactID = '16AB2B7C875A4667A62FCDC44079E99A';
  --WHERE ClientID IN ('CWALLACJ1298413358699','ADVCLIENTN04062');
  --WHERE Name LIKE '%Gresham Smith%'
-- FROM CFGClientType TABLE - **NOT NEEDED**
	--FROM [Vantagepoint].[dbo].[CFGClientType]
	--WHERE Name LIKE '%Graydon%'
	--WHERE 
-- FROM EMPLOYEE TABLE
	--FROM [dbo].[EMMain]
	--WHERE Employee = '05633'