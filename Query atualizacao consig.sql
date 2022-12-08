
 DROP TABLE IF EXISTS ##MESES_ADOBE_GERAL_ATUAL

SELECT 	convert(date,convert(varchar(4),year(convert(DATE,Date))) + '-' 
			+ convert(varchar(2),month(convert(DATE,Date))) + '-' + '01') AS Mes,
			convert(varchar(4),year(convert(DATE,Date))) + REPLICATE('0', 2 - LEN(convert(varchar(2),month(convert(DATE,Date))))) 
			+ RTrim(convert(varchar(2),month(convert(DATE,Date)))) AS Base

INTO ##MESES_ADOBE_GERAL_ATUAL 

FROM bi..tb_DA_Consignado_Adobe WITH (NOLOCK)

-- Tirando duplicidade + apagando tabela com duplicidades (00:00:10)

DROP TABLE IF EXISTS ##MESES_ADOBE_ATUAL

SELECT	DISTINCT Mes,
				Base

INTO ##MESES_ADOBE_ATUAL

FROM ##MESES_ADOBE_GERAL_ATUAL

DROP TABLE ##MESES_ADOBE_GERAL_ATUAL

-- Trazer os motivos de reprova

-- Selecionar as propostas com status atual REP de 2021 em diante (00:00:30)

--DROP TABLE IF EXISTS ##tb_DA_Consignado_Propostas_Reprovadas

--SELECT	NuProposta,
--		dtStProposta,
--		cdEsteira,
--		nuAtividade

--into ##tb_DA_Consignado_Propostas_Reprovadas

--from [panfdbp3042G\G].[prdconsignado].[dbo].[tbproposta] NOLOCK

--where StProposta = 'REP' and dtentrada >= '2021-01-01'

---- Selecionar o motivo de reprova do status REP atual (00:00:20)

--DROP TABLE IF EXISTS ##tb_DA_Consignado_Propostas_Adobe_Reprovados_Motivos 

--select	A.NuProposta, -- Nº Proposta
--		A.dtStProposta, -- Data da atividade
--		A.cdEsteira, -- Codigo da decisão/atividade (MPCODDECI)
--		A.NuAtividade, -- Codigo da decisão/atividade (MPNRDECI)
--		B.TDDESCR as DescricaoAtividade, -- Descrição da decisão/atividade
--		--B.TDOBJETIVO as ObjetivoAtividade -- Descritivo sobre a decisão/atividade
--		GETDATE() AS DtAtualizacao

--into ##tb_DA_Consignado_Propostas_Adobe_Reprovados_Motivos

--from ##tb_DA_Consignado_Propostas_Reprovadas A WITH(NOLOCK)  -- [panfdbp3047].[propostautpan].[sysfunc].[CMOVP] -- status atual de cada proposta

--LEFT JOIN [panfdbp3047].[propostautpan].[sysfunc].[TDECI] B WITH(NOLOCK) -- Descrições das atividades da esteira
--        on A.cdEsteira = B.TDCODDECI
--       And A.NuAtividade = B.TDNRDECI

--Inner join [panfdbp3047].[propostautpan].[sysfunc].[EPROP] NOLOCK C -- Esteira completa
--        on A.MPNRPROP = C.EPNRPROP

-- Selecionar os dados das propostas/digitações de consig (00:06:00)

DROP TABLE IF EXISTS ##tb_DA_Consignado_PropostasB2C_Atual

SELECT	PROP.dtEntrada,
		PROP.nuProposta,
		PROP.nmVendedor,
		PROP.stProposta,
		PROP.VrProducao,
		CONVENIO.NmConvenio,
		CONVENIO.NmConvenioGrupo01,
		CASE WHEN CONVENIO.NmConvenioGrupo01 = 'EP FGTS' THEN 'EP FGTS' ELSE OPERACAO.DsTipoOperacaoGrupo02 END DsTipoOperacaoGrupo02, -- Segregar as portabilidades do EP FGTS das demais
		CLIENTE.nuCpf,
		CONVERT(FLOAT,CLIENTE.nuCpf) AS nuCpfnum,
		CLIENTE.idCliente,
		CLIENTE.nmUF AS UFCliente,
		CORRESPONDENTE.NmCorrespondente,
		CORRESPONDENTE.DsTipoCanalDeVenda,
		CORRESPONDENTE.NmUF AS UFCorrespondente,
		--OPERADORA.operadora,
		CONVERT(FLOAT,NULL) AS ID_CLIENTE_360, -- tipo: float. Quando for nulo, são clientes que não aceitam os cookies e aí não conseguimos criar o cli id deles
		--REPROVA.DescricaoAtividade as MotivoReprova,
		CONTRATO.DtContrato,
		PROP.dtStProposta

into ##tb_DA_Consignado_PropostasB2C_Atual

from [panfdbp3042G\G].[prdconsignado].[dbo].[tbproposta] AS PROP WITH(NOLOCK) 

	left join [panfdbp3042G\G].[prdconsignado].[dbo].[vwcorrespondente] AS CORRESPONDENTE WITH(NOLOCK) 
		on PROP.cdcorrespondente = CORRESPONDENTE.cdcorrespondente

	left join [panfdbp3042G\G].[prdconsignado].[dbo].[vwconvenio] AS CONVENIO WITH(NOLOCK) 
		on PROP.cdconvenio = CONVENIO.cdconvenio

	left join [panfdbp3042G\G].[prdconsignado].[dbo].[VwTipoOperacao] AS OPERACAO WITH(NOLOCK) 
		on PROP.cdTipoOperacao = OPERACAO.CdTipoOperacao

	left join [panfdbp3042G\G].[prdconsignado].[dbo].[tbcliente] AS CLIENTE WITH(NOLOCK) 
		on PROP.idCliente = CLIENTE.idCliente

	--left join [panfdbp3042G\G].[dbcdc].[planej].[tbTableauPropostasIntegradasSuporteFilial_Analitico] AS OPERADORA WITH(NOLOCK) 
		--on PROP.nuProposta = OPERADORA.nuProposta

	--left join ##tb_DA_Consignado_Propostas_Adobe_Reprovados_Motivos AS REPROVA WITH(NOLOCK) 
		--on PROP.nuProposta = REPROVA.nuProposta

	left join [panfdbp3042G\G].[prdconsignado].[dbo].[tbcontrato] AS CONTRATO WITH(NOLOCK) 
		on PROP.nuProposta = LEFT(CONTRATO.nuContrato,9)

where	PROP.baseEntrada IN (SELECT Base collate SQL_Latin1_General_CP1_CI_AS FROM ##MESES_ADOBE_ATUAL WHERE Base <> '202103') AND 
		CORRESPONDENTE.DsTipoCanalDeVenda in ('Filial','Auto Contratacao') AND
		OPERACAO.DsTipoOperacaoGrupo02 <> 'Cartao' AND -- desconsiderar cartão pq nao tem registro na tabela de contrato
		PROP.dtEntrada <> (SELECT CONVERT(DATE,GETDATE())) -- não retornar propostas com data igual a data de execução do script

-- Identificar as duplicidades das propostas de EP FGTS

-- 1º) Inserir os dados das propostas "INT" na tabela sem duplicidades de CPF + DATA + VR + ST PROPOSTA

DROP TABLE IF EXISTS ##DA_PROPOSTAS_SEMDUPLICIDADE_FGTS_NUPROPOSTA

SELECT	NuProposta,
		nucpfnum,
		dtentrada,
		vrproducao,
		stproposta

INTO ##DA_PROPOSTAS_SEMDUPLICIDADE_FGTS_NUPROPOSTA

FROM ##tb_DA_Consignado_PropostasB2C_Atual

WHERE dstipooperacaogrupo02 = 'EP FGTS'
AND stproposta = 'INT'

-- 2º) Inserir os dados das propostas com status diferente de "INT" sem duplicidades de CPF + DATA + VR

INSERT INTO ##DA_PROPOSTAS_SEMDUPLICIDADE_FGTS_NUPROPOSTA

SELECT	A.NuProposta,
		A.nucpfnum,
		A.dtentrada,
		A.vrproducao,
		A.stproposta

FROM

(
SELECT	NuProposta,
		nucpfnum,
		dtentrada,
		vrproducao,
		stproposta,
		ROW_NUMBER() OVER(PARTITION BY nucpfnum, dtentrada, vrproducao ORDER BY nucpfnum, NuProposta DESC) AS ID_Proposta -- id por CPF + DATA + VALOR, ordenando pelo maior numero de proposta obtida da combinação

FROM ##tb_DA_Consignado_PropostasB2C_Atual

WHERE dstipooperacaogrupo02 = 'EP FGTS'
AND stproposta <> 'INT'

) AS A

	LEFT JOIN (SELECT	nucpfnum,
						dtentrada,
						vrproducao,
						COUNT(1) AS QDE

				FROM ##DA_PROPOSTAS_SEMDUPLICIDADE_FGTS_NUPROPOSTA

				GROUP BY nucpfnum,
						dtentrada,
						vrproducao) as B -- verificar se a combinação CPF + DATA + VR já existe

		ON A.nucpfnum		= B.nucpfnum -- verificar se a combinação CPF + DATA + VR já existe
		AND A.dtentrada		= B.dtentrada -- verificar se a combinação CPF + DATA + VR já existe
		AND A.vrproducao	= B.vrproducao -- verificar se a combinação CPF + DATA + VR já existe

WHERE A.ID_Proposta = 1 -- Retornar o maior numero de proposta caso haja duplicidade de CPF + DATA + VR
AND B.QDE IS NULL -- Não havendo propostas com a combinação de CPF + DATA + VR, as propostas desse status serão inseridas na tabela final sem duplicidades

-- Validação após execucação da rotina

-- Tabela sem duplicidades da rotina
--SELECT count(distinct nucpfnum) AS QdeCpfsDistintos, count(nuproposta) AS QdePropostas FROM ##PROPOSTAS_SEMDUPLICIDADE_FGTS_NUPROPOSTA
--SELECT stproposta, count(nuproposta) as QdePropostas from ##PROPOSTAS_SEMDUPLICIDADE_FGTS_NUPROPOSTA group by stproposta

---- Tabela original da rotina do Tableau
--SELECT count(distinct nucpfnum) AS QdeCpfsDistintos, count(nuproposta) AS QdePropostas FROM ##tb_DA_Consignado_PropostasB2C_Atual where dstipooperacaogrupo02 = 'EP FGTS'
--SELECT stproposta, count(nuproposta) as QdePropostas FROM ##tb_DA_Consignado_PropostasB2C_Atual where dstipooperacaogrupo02 = 'EP FGTS' group by stproposta

-- 3º) Deletar as propostas de FGTS que não permaneceram no step acima

DELETE FROM ##tb_DA_Consignado_PropostasB2C_Atual

WHERE dstipooperacaogrupo02 = 'EP FGTS' and nuProposta not in (SELECT nuProposta FROM ##DA_PROPOSTAS_SEMDUPLICIDADE_FGTS_NUPROPOSTA)

-- Identificar os CPFs distintos com digitação para localizar o ID CLI 360 na tabela do BI/DW (00:00:10)

DROP TABLE IF EXISTS ##tb_DA_Consignado_PropostasB2C_Atual_ID

SELECT DISTINCT nuCpfnum INTO ##tb_DA_Consignado_PropostasB2C_Atual_ID FROM ##tb_DA_Consignado_PropostasB2C_Atual

-- Trazer os dados do ID CLI 360 do BI/DW dos CPFs acima (tabela do BI atualizada 3x no dia) (00:15:00)

--DROP TABLE IF EXISTS ##CLI_CLIENTE360

--SELECT	*

--INTO ##CLI_CLIENTE360 

--FROM OPENQUERY (BI, 'SELECT ID_CLIENTE_360, CPF_CNPJ FROM BI_ODS.RL_CLI_CLIENTE360')

--WHERE CPF_CNPJ IN (SELECT nuCpfnum FROM ##tb_DA_Consignado_PropostasB2C_ID)

DROP TABLE IF EXISTS ##CLI_CLIENTE360_Atual

SELECT ID_CLIENTE_360,CPF_CNPJ

INTO ##CLI_CLIENTE360_Atual

FROM bi..RL_CLI_CLIENTE360
--FROM RL_CLI_CLIENTE360

WHERE CPF_CNPJ IN (SELECT nuCpfnum FROM ##tb_DA_Consignado_PropostasB2C_Atual_ID)

-- Atualizar os dados de ID CLI 360 com base no retorno do BI/DW (00:00:10)

UPDATE ##tb_DA_Consignado_PropostasB2C_Atual
SET

ID_CLIENTE_360 = B.ID_CLIENTE_360

FROM ##tb_DA_Consignado_PropostasB2C_Atual AS A

	LEFT JOIN ##CLI_CLIENTE360_Atual AS B
		ON A.nuCpfnum = B.CPF_CNPJ

-- Tratamento dos dados de adobe importados (extração DA_Adobe_Consignado_Producao da Adobe)

-- 1º) Trazer os Tracking Codes distintos (00:00:05)

DROP TABLE IF EXISTS ##TC_DISTINTO

SELECT [Tracking Code], COUNT (DATE) AS QDE INTO ##TC_DISTINTO FROM bi..tb_DA_Consignado_Adobe WITH (NOLOCK) GROUP BY [Tracking Code]

-- 2º) Trazer os Last Touch Channel Detail distintos (00:00:05)

DROP TABLE IF EXISTS ##LTCD_DISTINTO

SELECT [Last Touch Channel Detail], COUNT (DATE) AS QDE INTO ##LTCD_DISTINTO FROM bi..tb_DA_Consignado_Adobe WITH (NOLOCK) GROUP BY [Last Touch Channel Detail]

-- 3º) Aplicar os tratamentos do Tracking Code para "Empréstimo Consignado" e "EP FGTS" (00:01:00)

DROP TABLE IF EXISTS ##tb_DA_Consignado_Adobe_TC_Atual

SELECT	[Tracking Code],
		Qde,
		CASE 
			WHEN [Tracking Code]	LIKE '%c11:%'	OR [Tracking Code] LIKE '%c16:%' OR [Tracking Code] LIKE '%c17:%' THEN 'Afiliados'
			WHEN [Tracking Code]	LIKE '%c08:%'	OR [Tracking Code] LIKE '%c09:%' OR [Tracking Code] LIKE '%:c21%' THEN 'CRM'
			WHEN [Tracking Code]	LIKE '%BP:%'	OR [Tracking Code] LIKE '%None%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%https://www.bancopan.com.br%' AND [Tracking Code] NOT LIKE '%idcmp%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%:empty:%' THEN 'Orgânico + Direto'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta Consig'

			WHEN ([Tracking Code]	LIKE '%facebook%' OR [Tracking Code] LIKE '%criteo%' OR [Tracking Code]	LIKE '%verizon%'
				OR [Tracking Code]	LIKE '%inflr%' OR [Tracking Code] LIKE '%bing%') AND [Tracking Code] LIKE '%[_]cons[_]%' 
				AND [Tracking Code] NOT LIKE '%fgts%'AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta Consig'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') 
				AND ([Tracking Code] LIKE '%[_]autocontratacao[_]%' OR [Tracking Code] LIKE '%[_]consignado-%') THEN 'Mídia Paga - Conta Consig'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND [Tracking Code] NOT LIKE '%[_]ccdig[_]%' 
				AND [Tracking Code] NOT LIKE '%[_]conta-digital-%' AND [Tracking Code]	NOT LIKE '%[_]cart[_]%'
				AND [Tracking Code]	NOT LIKE '%[_]cartoes[_]%' AND [Tracking Code] NOT LIKE '%[_]cartoes-%' 
				AND [Tracking Code] NOT LIKE '%[_]autocontratacao[_]%' AND [Tracking Code] NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code]  LIKE '%[_]ccdig[_]%' 
				OR [Tracking Code]  LIKE '%[_]conta-digital-%' OR [Tracking Code] LIKE '%[_]cart[_]%'
				OR [Tracking Code]	LIKE '%[_]cartoes[_]%' OR [Tracking Code]  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] NOT LIKE '%[_]cons[_]%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN ([Tracking Code]	LIKE '%facebook%' OR [Tracking Code] LIKE '%criteo%' OR [Tracking Code]	LIKE '%verizon%'
				OR [Tracking Code]	LIKE '%inflr%' OR [Tracking Code] LIKE '%bing%' OR [Tracking Code] LIKE '%globo%'
				OR [Tracking Code] LIKE '%UOL%' OR [Tracking Code] LIKE '%tiktok%' OR [Tracking Code] LIKE '%shopback%'
				OR [Tracking Code] LIKE '%sbt%') AND [Tracking Code] NOT LIKE '%[_]cons[_]%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%[_]cons[_]%'  AND [Tracking Code] NOT LIKE '%fgts%' AND [Tracking Code] LIKE '%midiaoff%'THEN 'Mídia Offline - Consig'

		ELSE 'Outro' END AS TIPO_MIDIA_TC_CONSIGNADO, 

		CASE 
			WHEN [Tracking Code]	LIKE '%c11:%' THEN 'Afiliados - Outros produtos'
			WHEN [Tracking Code]	LIKE '%c16:%' THEN 'Afiliados PF'
			WHEN [Tracking Code]	LIKE '%c17:%' THEN 'Afiliados PJ'
			WHEN [Tracking Code]	LIKE '%c08:%'	OR [Tracking Code] LIKE '%c09:%' OR [Tracking Code] LIKE '%:c21%' THEN 'CRM'
			WHEN [Tracking Code]	LIKE '%BP:%'	OR [Tracking Code] LIKE '%None%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%https://www.bancopan.com.br%' AND [Tracking Code] NOT LIKE '%idcmp%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%:empty:%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Facebook'
			WHEN [Tracking Code]	LIKE '%criteo%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN [Tracking Code]	LIKE '%verizon%'	AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Tracking Code]	LIKE '%inflr%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Tracking Code]	LIKE '%bing%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Tracking Code]	LIKE '%leadmedia%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Tracking Code]	LIKE '%optimise%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'
			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') 
				AND ([Tracking Code] LIKE '%[_]autocontratacao[_]%' OR [Tracking Code] LIKE '%[_]consignado-%') THEN 'Google'			

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND [Tracking Code] NOT LIKE '%[_]ccdig[_]%' 
				AND [Tracking Code] NOT LIKE '%[_]conta-digital-%' AND [Tracking Code]	NOT LIKE '%[_]cart[_]%'
				AND [Tracking Code]	NOT LIKE '%[_]cartoes[_]%' AND [Tracking Code] NOT LIKE '%[_]cartoes-%' 
				AND [Tracking Code] NOT LIKE '%[_]autocontratacao[_]%' AND [Tracking Code] NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code]  LIKE '%[_]ccdig[_]%' 
				OR [Tracking Code]  LIKE '%[_]conta-digital-%' OR [Tracking Code] LIKE '%[_]cart[_]%'
				OR [Tracking Code]	LIKE '%[_]cartoes[_]%' OR [Tracking Code]  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] NOT LIKE '%[_]cons[_]%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN ([Tracking Code]	LIKE '%facebook%' OR [Tracking Code] LIKE '%criteo%' OR [Tracking Code]	LIKE '%verizon%'
				OR [Tracking Code]	LIKE '%inflr%' OR [Tracking Code] LIKE '%bing%' OR [Tracking Code] LIKE '%globo%'
				OR [Tracking Code] LIKE '%UOL%' OR [Tracking Code] LIKE '%tiktok%' OR [Tracking Code] LIKE '%shopback%'
				OR [Tracking Code] LIKE '%sbt%') AND [Tracking Code] NOT LIKE '%[_]cons[_]%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%[_]cons[_]%'  AND [Tracking Code] NOT LIKE '%fgts%' AND [Tracking Code] LIKE '%midiaoff%' AND [Tracking Code] LIKE '%metro%' THEN 'Mídia Offline - Metrô'

		ELSE 'Outro' END AS ORIGEM_MIDIA_TC_CONSIGNADO,

		CASE 
			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code]  LIKE '%[_]ccdig[_]%' 
				OR [Tracking Code]  LIKE '%[_]conta-digital-%' OR [Tracking Code] LIKE '%[_]cart[_]%'
				OR [Tracking Code]	LIKE '%[_]cartoes[_]%' OR [Tracking Code]  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') 
				AND ([Tracking Code] LIKE '%[_]autocontratacao[_]%' OR [Tracking Code] LIKE '%[_]consignado-%') THEN 'Google Search'	

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%inst%' THEN 'Google Search - Conta Inst'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Search'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%display%'
				AND [Tracking Code] NOT LIKE '%discovery%' AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%disp%'
				AND [Tracking Code] NOT LIKE '%discovery%' AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%discovery%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%gsp%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%[_]vid%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Geral'
			WHEN [Tracking Code]	LIKE '%criteo%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN [Tracking Code]	LIKE '%verizon%'	AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Tracking Code]	LIKE '%inflr%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Tracking Code]	LIKE '%bing%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Tracking Code]	LIKE '%leadmedia%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Tracking Code]	LIKE '%optimise%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'
		ELSE 'Outro' END AS TIPO_CAMPANHA_TC_CONSIGNADO,

		CASE
			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code]  LIKE '%[_]ccdig[_]%' 
				OR [Tracking Code]  LIKE '%[_]conta-digital-%' OR [Tracking Code] LIKE '%[_]cart[_]%'
				OR [Tracking Code]	LIKE '%[_]cartoes[_]%' OR [Tracking Code]  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') 
				AND ([Tracking Code] LIKE '%[_]autocontratacao[_]%' OR [Tracking Code] LIKE '%[_]consignado-%') THEN 'Search Consig - Produto + Marca'	

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%inst%' AND ([Tracking Code] LIKE '%institucional%' 
				OR [Tracking Code] LIKE '%panamericano%') THEN 'Search Institucional - Inst'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%inst%' AND [Tracking Code] LIKE '%atendimento%' THEN 'Search Institucional - Atendimento'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%inst%' THEN 'Search Institucional - Outras Campanhas' -- o que sobrar de "inst" cai em outras campanhas

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' AND ([Tracking Code] LIKE '%marca%' 
				OR [Tracking Code] LIKE '%produto-marca%') THEN 'Search Consig - Produto + Marca'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' AND ([Tracking Code] LIKE '%genericas%' 
				OR [Tracking Code] LIKE '%generica%') THEN 'Search Consig - Genéricas'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' AND ([Tracking Code] LIKE '%concorrentes%' 
				OR [Tracking Code] LIKE '%concorrente%') THEN 'Search Consig - Concorrentes'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Search Consig - Outros'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%display%'
				AND [Tracking Code] NOT LIKE '%discovery%' AND [Tracking Code] LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%disp%'
				AND [Tracking Code] NOT LIKE '%discovery%' AND [Tracking Code] LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%discovery%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%gsp%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%[_]vid%'
				AND [Tracking Code] LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%all-placements%' AND [Tracking Code]	LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - All Placements'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%post-feed%' AND [Tracking Code]		LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Post feed'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%stories%' AND [Tracking Code]		LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Stories/Reels'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%carrossel%' AND [Tracking Code]		LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Carrossel'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%lead-ad%' AND [Tracking Code]		LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Lead Ad'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%click-message%' AND [Tracking Code]	LIKE '%[_]cons[_]%' 
				AND [Tracking Code]	NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Click Message'

			WHEN [Tracking Code]	LIKE '%criteo%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Criteo - Retargeting'

			WHEN [Tracking Code]	LIKE '%verizon%'	AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'

			WHEN [Tracking Code]	LIKE '%inflr%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'

			WHEN [Tracking Code]	LIKE '%bing%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Bing'

				WHEN [Tracking Code]	LIKE '%leadmedia%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'

				WHEN [Tracking Code]	LIKE '%optimise%'		AND [Tracking Code]	LIKE '%[_]cons[_]%' AND [Tracking Code]	NOT LIKE '%fgts%'
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

		ELSE 'Outro' END AS CAMPANHA_TC_CONSIGNADO,

		CASE 
			WHEN [Tracking Code]	LIKE '%awa%'		THEN 'Awareness'
			WHEN [Tracking Code]	LIKE '%:cons[_]%'	THEN 'Consideração'
			WHEN [Tracking Code]	LIKE '%perf%'		THEN 'Performance'
			WHEN [Tracking Code]	LIKE '%engaj%'		THEN 'Engajamento'
		ELSE 'Outro' END AS OBJETIVO_CAMPANHA_TC_CONSIGNADO,

		CASE 
			WHEN [Tracking Code]	LIKE '%mar-aberto%'										THEN 'Mar aberto'
			WHEN [Tracking Code]	LIKE '%interesse%'										THEN 'Interesse'
			WHEN [Tracking Code]	LIKE '%base-3party%'									THEN 'Base-3party'
			WHEN [Tracking Code]	LIKE '%lal%'											THEN 'LAL'
			WHEN [Tracking Code]	LIKE '%rmkt%' OR [Tracking Code] LIKE '%remarketing%'	THEN 'Remarketing'
			WHEN [Tracking Code]	LIKE '%base-1party%' OR [Tracking Code]	LIKE '%base%'	THEN 'Base-1party'
		ELSE 'Outras' END AS SEGMENTACAO_TC_CONSIGNADO,

		CASE 
			WHEN [Tracking Code]	LIKE '%lp-inss%' THEN 'LP INSS'
			WHEN [Tracking Code]	LIKE '%lp-siape%' THEN 'LP SIAPE'
			WHEN [Tracking Code]	LIKE '%lp-exercito%' THEN 'LP EXÉRCITO'
			WHEN [Tracking Code]	LIKE '%pdp-site%' THEN 'PDP SITE'
			WHEN [Tracking Code]	LIKE '%lp-fgts%'  THEN 'LP FGTS'
			WHEN [Tracking Code]	LIKE '%funil-convenio%' THEN 'FUNIL CONVÊNIO'
			WHEN [Tracking Code]	LIKE '%funil-ident%' THEN 'FUNIL IDENT'
			WHEN [Tracking Code]	LIKE '%whatsapp%' THEN 'WHATSAPP'
		ELSE 'Outras' END AS URL_DESTINO_TC_CONSIGNADO,
		
		CASE 
			WHEN [Tracking Code]	LIKE '%c11:%'	OR [Tracking Code] LIKE '%c16:%' OR [Tracking Code] LIKE '%c17:%' THEN 'Afiliados'
			WHEN [Tracking Code]	LIKE '%c08:%'	OR [Tracking Code] LIKE '%c09:%' OR [Tracking Code] LIKE '%:c21%' THEN 'CRM'
			WHEN [Tracking Code]	LIKE '%BP:%'	OR [Tracking Code] LIKE '%None%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%https://www.bancopan.com.br%' AND [Tracking Code] NOT LIKE '%idcmp%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%:empty:%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta FGTS'

			WHEN ([Tracking Code]	LIKE '%facebook%' OR [Tracking Code] LIKE '%criteo%' OR [Tracking Code]	LIKE '%verizon%'
				OR [Tracking Code]	LIKE '%inflr%' OR [Tracking Code] LIKE '%bing%') AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta FGTS'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND [Tracking Code] NOT LIKE '%[_]ccdig[_]%' 
				AND [Tracking Code] NOT LIKE '%[_]conta-digital-%' AND [Tracking Code]	NOT LIKE '%[_]cart[_]%'
				AND [Tracking Code]	NOT LIKE '%[_]cartoes[_]%' AND [Tracking Code] NOT LIKE '%[_]cartoes-%'
				AND [Tracking Code]	NOT LIKE '%[_]autocontratacao[_]%' AND [Tracking Code] NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code] LIKE '%[_]autocontratacao[_]%' 
				OR [Tracking Code]  LIKE '%[_]consignado-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code]  LIKE '%[_]ccdig[_]%' 
				OR [Tracking Code]  LIKE '%[_]conta-digital-%' OR [Tracking Code] LIKE '%[_]cart[_]%'
				OR [Tracking Code]	LIKE '%[_]cartoes[_]%' OR [Tracking Code]  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN ([Tracking Code]	LIKE '%facebook%' OR [Tracking Code] LIKE '%criteo%' OR [Tracking Code]	LIKE '%verizon%'
				OR [Tracking Code]	LIKE '%inflr%' OR [Tracking Code]	LIKE '%optimise%' OR [Tracking Code]	LIKE '%leadmedia%' 
				OR [Tracking Code] LIKE '%bing%' OR [Tracking Code] LIKE '%globo%'
				OR [Tracking Code] LIKE '%UOL%' OR [Tracking Code] LIKE '%tiktok%' OR [Tracking Code] LIKE '%shopback%'
				OR [Tracking Code] LIKE '%sbt%') AND [Tracking Code] NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] LIKE '%midiaoff%' THEN 'Mídia Offline - FGTS'

		ELSE 'Outro' END AS TIPO_MIDIA_TC_FGTS, 

		CASE 
			WHEN [Tracking Code]	LIKE '%c11:%' THEN 'Afiliados - Outros produtos'
			WHEN [Tracking Code]	LIKE '%c16:%' THEN 'Afiliados PF'
			WHEN [Tracking Code]	LIKE '%c17:%' THEN 'Afiliados PJ'
			WHEN [Tracking Code]	LIKE '%c08:%'	OR [Tracking Code] LIKE '%c09:%' OR [Tracking Code] LIKE '%:c21%' THEN 'CRM'
			WHEN [Tracking Code]	LIKE '%BP:%'	OR [Tracking Code] LIKE '%None%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%https://www.bancopan.com.br%' AND [Tracking Code] NOT LIKE '%idcmp%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%:empty:%' THEN 'Orgânico + Direto'
			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Facebook'
			WHEN [Tracking Code]	LIKE '%criteo%'		AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN [Tracking Code]	LIKE '%verizon%'	AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Tracking Code]	LIKE '%inflr%'		AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Tracking Code]	LIKE '%bing%'		AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Tracking Code]	LIKE '%leadmedia%'		AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Tracking Code]	LIKE '%optimise%'		AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'
			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND [Tracking Code] NOT LIKE '%[_]ccdig[_]%' 
				AND [Tracking Code] NOT LIKE '%[_]conta-digital-%' AND [Tracking Code]	NOT LIKE '%[_]cart[_]%'
				AND [Tracking Code]	NOT LIKE '%[_]cartoes[_]%' AND [Tracking Code] NOT LIKE '%[_]cartoes-%'
				AND [Tracking Code]	NOT LIKE '%[_]autocontratacao[_]%' AND [Tracking Code] NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code] LIKE '%[_]autocontratacao[_]%' 
				OR [Tracking Code]  LIKE '%[_]consignado-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code]  LIKE '%[_]ccdig[_]%' 
				OR [Tracking Code]  LIKE '%[_]conta-digital-%' OR [Tracking Code] LIKE '%[_]cart[_]%'
				OR [Tracking Code]	LIKE '%[_]cartoes[_]%' OR [Tracking Code]  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN ([Tracking Code]	LIKE '%facebook%' OR [Tracking Code] LIKE '%criteo%' OR [Tracking Code]	LIKE '%verizon%'
				OR [Tracking Code]	LIKE '%inflr%' OR [Tracking Code]	LIKE '%optimise%' OR [Tracking Code]	LIKE '%leadmedia%' 
				OR [Tracking Code] LIKE '%bing%' OR [Tracking Code] LIKE '%globo%'
				OR [Tracking Code] LIKE '%UOL%' OR [Tracking Code] LIKE '%tiktok%' OR [Tracking Code] LIKE '%shopback%'
				OR [Tracking Code] LIKE '%sbt%') AND [Tracking Code] NOT LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] LIKE '%midiaoff%' AND [Tracking Code] LIKE '%metro%' THEN 'Mídia Offline - Metrô'

		ELSE 'Outro' END AS ORIGEM_MIDIA_TC_FGTS,

		CASE 
			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code]  LIKE '%[_]ccdig[_]%' 
				OR [Tracking Code]  LIKE '%[_]conta-digital-%' OR [Tracking Code] LIKE '%[_]cart[_]%'
				OR [Tracking Code]	LIKE '%[_]cartoes[_]%' OR [Tracking Code]  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') 
				AND ([Tracking Code] LIKE '%[_]autocontratacao[_]%' OR [Tracking Code] LIKE '%[_]consignado-%') THEN 'Outro'	

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%inst%' THEN 'Google Search - Conta Inst'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Search'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%display%'
				AND [Tracking Code] NOT LIKE '%discovery%' AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%disp%'
				AND [Tracking Code] NOT LIKE '%discovery%' AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%discovery%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%gsp%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%[_]vid%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Geral'
			WHEN [Tracking Code]	LIKE '%criteo%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN [Tracking Code]	LIKE '%verizon%'	AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Tracking Code]	LIKE '%inflr%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Tracking Code]	LIKE '%bing%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Tracking Code]	LIKE '%leadmedia%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Tracking Code]	LIKE '%optimise%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'
		ELSE 'Outro' END AS TIPO_CAMPANHA_TC_FGTS,

		CASE
			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') AND ([Tracking Code]  LIKE '%[_]ccdig[_]%' 
				OR [Tracking Code]  LIKE '%[_]conta-digital-%' OR [Tracking Code] LIKE '%[_]cart[_]%'
				OR [Tracking Code]	LIKE '%[_]cartoes[_]%' OR [Tracking Code]  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND ([Tracking Code] LIKE '%inst%' OR [Tracking Code] LIKE '%brand%') 
				AND ([Tracking Code] LIKE '%[_]autocontratacao[_]%' OR [Tracking Code] LIKE '%[_]consignado-%') THEN 'Outro'	
		
			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%inst%' AND ([Tracking Code] LIKE '%institucional%' 
				OR [Tracking Code] LIKE '%panamericano%') THEN 'Search Institucional - Inst'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%inst%' AND [Tracking Code] LIKE '%atendimento%' THEN 'Search Institucional - Atendimento'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%inst%' THEN 'Search Institucional - Outras Campanhas' -- o que sobrar de "inst" cai em outras campanhas

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' AND ([Tracking Code] LIKE '%marca%' 
				OR [Tracking Code] LIKE '%produto-marca%') THEN 'Search FGTS - Produto + Marca'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' AND ([Tracking Code] LIKE '%genericas%' 
				OR [Tracking Code] LIKE '%generica%') THEN 'Search FGTS - Genéricas'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' AND ([Tracking Code] LIKE '%concorrentes%' 
			OR [Tracking Code] LIKE '%concorrente%') THEN 'Search FGTS - Concorrentes'	

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%srch%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Search FGTS - Outros'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%display%'
				AND [Tracking Code] NOT LIKE '%discovery%' AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%disp%'
				AND [Tracking Code] NOT LIKE '%discovery%' AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%discovery%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%gsp%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Tracking Code]	LIKE '%google%' AND [Tracking Code] NOT LIKE '%:empty%' AND [Tracking Code] LIKE '%[_]vid%'
				AND [Tracking Code] LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%all-placements%' AND [Tracking Code]	LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - All Placements'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%post-feed%' AND [Tracking Code]		LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Post feed'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%stories%' AND [Tracking Code]		LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Stories/Reels'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%carrossel%' AND [Tracking Code]		LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Carrossel'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%lead-ad%' AND [Tracking Code]		LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Lead Ad'

			WHEN [Tracking Code]	LIKE '%facebook%'	AND [Tracking Code]	LIKE '%click-message%' AND [Tracking Code]	LIKE '%fgts%' 
				AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'FB - Click Message'

			WHEN [Tracking Code]	LIKE '%criteo%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Criteo - Retargeting'
			WHEN [Tracking Code]	LIKE '%verizon%'	AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Tracking Code]	LIKE '%inflr%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Tracking Code]	LIKE '%bing%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Tracking Code]	LIKE '%leadmedia%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Tracking Code]	LIKE '%optimise%'		AND [Tracking Code]	LIKE '%fgts%' AND [Tracking Code] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

		ELSE 'Outro' END AS CAMPANHA_TC_FGTS,

		CASE 
			WHEN [Tracking Code]	LIKE '%awa%'		THEN 'Awareness'
			WHEN [Tracking Code]	LIKE '%:cons[_]%'	THEN 'Consideração'
			WHEN [Tracking Code]	LIKE '%perf%'		THEN 'Performance'
			WHEN [Tracking Code]	LIKE '%engaj%'		THEN 'Engajamento'
		ELSE 'Outro' END AS OBJETIVO_CAMPANHA_TC_FGTS,

		CASE 
			WHEN [Tracking Code]	LIKE '%mar-aberto%'										THEN 'Mar aberto'
			WHEN [Tracking Code]	LIKE '%interesse%'										THEN 'Interesse'
			WHEN [Tracking Code]	LIKE '%base-3party%'									THEN 'Base-3party'
			WHEN [Tracking Code]	LIKE '%lal%'											THEN 'LAL'
			WHEN [Tracking Code]	LIKE '%rmkt%' OR [Tracking Code] LIKE '%remarketing%'	THEN 'Remarketing'
			WHEN [Tracking Code]	LIKE '%base-1party%' OR [Tracking Code]	LIKE '%base%'	THEN 'Base-1party'
		ELSE 'Outras' END AS SEGMENTACAO_TC_FGTS,

		CASE 
			WHEN [Tracking Code]	LIKE '%lp-inss%' THEN 'LP INSS'
			WHEN [Tracking Code]	LIKE '%lp-siape%' THEN 'LP SIAPE'
			WHEN [Tracking Code]	LIKE '%lp-exercito%' THEN 'LP EXÉRCITO'
			WHEN [Tracking Code]	LIKE '%pdp-site%' THEN 'PDP SITE'
			WHEN [Tracking Code]	LIKE '%lp-fgts%'  THEN 'LP FGTS'
			WHEN [Tracking Code]	LIKE '%funil-convenio%' THEN 'FUNIL CONVÊNIO'
			WHEN [Tracking Code]	LIKE '%funil-ident%' THEN 'FUNIL IDENT'
			WHEN [Tracking Code]	LIKE '%whatsapp%' THEN 'WHATSAPP'
		ELSE 'Outras' END AS URL_DESTINO_TC_FGTS

INTO ##tb_DA_Consignado_Adobe_TC_Atual

FROM

(SELECT * FROM ##TC_DISTINTO) AS A

-- 4º) Aplicar os tratamentos do Last Touch Channel Detail para "Empréstimo Consignado" e "EP FGTS" (00:01:00)

DROP TABLE IF EXISTS ##tb_DA_Consignado_Adobe_LTCD_Atual

SELECT	[Last Touch Channel Detail],
		Qde,
		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%c11:%'	OR [Last Touch Channel Detail] LIKE '%c16:%' OR [Last Touch Channel Detail] LIKE '%c17:%' THEN 'Afiliados'
			WHEN [Last Touch Channel Detail]	LIKE '%c08:%'	OR [Last Touch Channel Detail] LIKE '%c09:%' OR [Last Touch Channel Detail] LIKE '%:c21%' THEN 'CRM'
			WHEN [Last Touch Channel Detail]	LIKE '%BP:%'	OR [Last Touch Channel Detail] LIKE '%None%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%https://www.bancopan.com.br%' AND [Last Touch Channel Detail] NOT LIKE '%idcmp%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%:empty:%' THEN 'Orgânico + Direto'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta Consig'

			WHEN ([Last Touch Channel Detail]	LIKE '%facebook%' OR [Last Touch Channel Detail] LIKE '%criteo%' OR [Last Touch Channel Detail]	LIKE '%verizon%'
				OR [Last Touch Channel Detail]	LIKE '%inflr%' OR [Last Touch Channel Detail] LIKE '%bing%') AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail] NOT LIKE '%fgts%'AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta Consig'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') 
				AND ([Last Touch Channel Detail] LIKE '%[_]autocontratacao[_]%' OR [Last Touch Channel Detail] LIKE '%[_]consignado-%') THEN 'Mídia Paga - Conta Consig'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND [Last Touch Channel Detail] NOT LIKE '%[_]ccdig[_]%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]conta-digital-%' AND [Last Touch Channel Detail]	NOT LIKE '%[_]cart[_]%'
				AND [Last Touch Channel Detail]	NOT LIKE '%[_]cartoes[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]cartoes-%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]autocontratacao[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail]  LIKE '%[_]ccdig[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]conta-digital-%' OR [Last Touch Channel Detail] LIKE '%[_]cart[_]%'
				OR [Last Touch Channel Detail]	LIKE '%[_]cartoes[_]%' OR [Last Touch Channel Detail]  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] NOT LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN ([Last Touch Channel Detail]	LIKE '%facebook%' OR [Last Touch Channel Detail] LIKE '%criteo%' OR [Last Touch Channel Detail]	LIKE '%verizon%'
				OR [Last Touch Channel Detail]	LIKE '%inflr%' OR [Last Touch Channel Detail] LIKE '%bing%' OR [Last Touch Channel Detail] LIKE '%leadmedia%'
				OR [Last Touch Channel Detail] LIKE '%optimise%' OR [Last Touch Channel Detail] LIKE '%globo%'
				OR [Last Touch Channel Detail] LIKE '%UOL%' OR [Last Touch Channel Detail] LIKE '%tiktok%' OR [Last Touch Channel Detail] LIKE '%shopback%'
				OR [Last Touch Channel Detail] LIKE '%sbt%') AND [Last Touch Channel Detail] NOT LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%[_]cons[_]%'  AND [Last Touch Channel Detail] NOT LIKE '%fgts%' AND [Last Touch Channel Detail] LIKE '%midiaoff%' THEN 'Mídia Offline - Consig'

		ELSE 'Outro' END AS TIPO_MIDIA_LTCD_CONSIGNADO, 

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%c11:%' THEN 'Afiliados - Outros produtos'
			WHEN [Last Touch Channel Detail]	LIKE '%c16:%' THEN 'Afiliados PF'
			WHEN [Last Touch Channel Detail]	LIKE '%c17:%' THEN 'Afiliados PJ'
			WHEN [Last Touch Channel Detail]	LIKE '%c08:%'	OR [Last Touch Channel Detail] LIKE '%c09:%' OR [Last Touch Channel Detail] LIKE '%:c21%' THEN 'CRM'
			WHEN [Last Touch Channel Detail]	LIKE '%BP:%'	OR [Last Touch Channel Detail] LIKE '%None%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%https://www.bancopan.com.br%' AND [Last Touch Channel Detail] NOT LIKE '%idcmp%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%:empty:%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Facebook'
			WHEN [Last Touch Channel Detail]	LIKE '%criteo%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN [Last Touch Channel Detail]	LIKE '%verizon%'	AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Last Touch Channel Detail]	LIKE '%inflr%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Last Touch Channel Detail]	LIKE '%bing%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Last Touch Channel Detail]	LIKE '%leadmedia%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Last Touch Channel Detail]	LIKE '%optimise%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') 
				AND ([Last Touch Channel Detail] LIKE '%[_]autocontratacao[_]%' OR [Last Touch Channel Detail] LIKE '%[_]consignado-%') THEN 'Google'			

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND [Last Touch Channel Detail] NOT LIKE '%[_]ccdig[_]%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]conta-digital-%' AND [Last Touch Channel Detail]	NOT LIKE '%[_]cart[_]%'
				AND [Last Touch Channel Detail]	NOT LIKE '%[_]cartoes[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]cartoes-%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]autocontratacao[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail]  LIKE '%[_]ccdig[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]conta-digital-%' OR [Last Touch Channel Detail] LIKE '%[_]cart[_]%'
				OR [Last Touch Channel Detail]	LIKE '%[_]cartoes[_]%' OR [Last Touch Channel Detail]  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] NOT LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN ([Last Touch Channel Detail]	LIKE '%facebook%' OR [Last Touch Channel Detail] LIKE '%criteo%' OR [Last Touch Channel Detail]	LIKE '%verizon%'
				OR [Last Touch Channel Detail]	LIKE '%inflr%' OR [Last Touch Channel Detail] LIKE '%bing%' OR [Last Touch Channel Detail] LIKE '%leadmedia%'
				OR [Last Touch Channel Detail] LIKE '%optimise%' OR [Last Touch Channel Detail] LIKE '%globo%'
				OR [Last Touch Channel Detail] LIKE '%UOL%' OR [Last Touch Channel Detail] LIKE '%tiktok%' OR [Last Touch Channel Detail] LIKE '%shopback%'
				OR [Last Touch Channel Detail] LIKE '%sbt%') AND [Last Touch Channel Detail] NOT LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%[_]cons[_]%'  AND [Last Touch Channel Detail] NOT LIKE '%fgts%' AND [Last Touch Channel Detail] LIKE '%midiaoff%' AND [Last Touch Channel Detail] LIKE '%metro%' THEN 'Mídia Offline - Metrô'

		ELSE 'Outro' END AS ORIGEM_MIDIA_LTCD_CONSIGNADO,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail]  LIKE '%[_]ccdig[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]conta-digital-%' OR [Last Touch Channel Detail] LIKE '%[_]cart[_]%'
				OR [Last Touch Channel Detail]	LIKE '%[_]cartoes[_]%' OR [Last Touch Channel Detail]  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') 
				AND ([Last Touch Channel Detail] LIKE '%[_]autocontratacao[_]%' OR [Last Touch Channel Detail] LIKE '%[_]consignado-%') THEN 'Google Search'	

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%inst%' THEN 'Google Search - Conta Inst'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Search'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%display%'
				AND [Last Touch Channel Detail] NOT LIKE '%discovery%' AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%disp%'
				AND [Last Touch Channel Detail] NOT LIKE '%discovery%' AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%discovery%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%gsp%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%[_]vid%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Geral'
			WHEN [Last Touch Channel Detail]	LIKE '%criteo%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN [Last Touch Channel Detail]	LIKE '%verizon%'	AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Last Touch Channel Detail]	LIKE '%inflr%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Last Touch Channel Detail]	LIKE '%bing%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Last Touch Channel Detail]	LIKE '%leadmedia%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Last Touch Channel Detail]	LIKE '%optimise%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'
		ELSE 'Outro' END AS TIPO_CAMPANHA_LTCD_CONSIGNADO,

		CASE
			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail]  LIKE '%[_]ccdig[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]conta-digital-%' OR [Last Touch Channel Detail] LIKE '%[_]cart[_]%'
				OR [Last Touch Channel Detail]	LIKE '%[_]cartoes[_]%' OR [Last Touch Channel Detail]  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') 
				AND ([Last Touch Channel Detail] LIKE '%[_]autocontratacao[_]%' OR [Last Touch Channel Detail] LIKE '%[_]consignado-%') THEN 'Search Consig - Produto + Marca'	

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%inst%' AND ([Last Touch Channel Detail] LIKE '%institucional%' 
				OR [Last Touch Channel Detail] LIKE '%panamericano%') THEN 'Search Institucional - Inst'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%inst%' AND [Last Touch Channel Detail] LIKE '%atendimento%' THEN 'Search Institucional - Atendimento'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%inst%' THEN 'Search Institucional - Outras Campanhas' -- o que sobrar de "inst" cai em outras campanhas

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' AND ([Last Touch Channel Detail] LIKE '%marca%' 
				OR [Last Touch Channel Detail] LIKE '%produto-marca%') THEN 'Search Consig - Produto + Marca'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' AND ([Last Touch Channel Detail] LIKE '%genericas%' 
				OR [Last Touch Channel Detail] LIKE '%generica%') THEN 'Search Consig - Genéricas'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' AND ([Last Touch Channel Detail] LIKE '%concorrentes%' 
				OR [Last Touch Channel Detail] LIKE '%concorrente%') THEN 'Search Consig - Concorrentes'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Search Consig - Outros'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%display%'
				AND [Last Touch Channel Detail] NOT LIKE '%discovery%' AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%disp%'
				AND [Last Touch Channel Detail] NOT LIKE '%discovery%' AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%discovery%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%gsp%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%[_]vid%'
				AND [Last Touch Channel Detail] LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%all-placements%' AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - All Placements'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%post-feed%' AND [Last Touch Channel Detail]		LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Post feed'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%stories%' AND [Last Touch Channel Detail]		LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Stories/Reels'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%carrossel%' AND [Last Touch Channel Detail]		LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Carrossel'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%lead-ad%' AND [Last Touch Channel Detail]		LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Lead Ad'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%click-message%' AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' 
				AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Click Message'

			WHEN [Last Touch Channel Detail]	LIKE '%criteo%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Criteo - Retargeting'

			WHEN [Last Touch Channel Detail]	LIKE '%verizon%'	AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'

			WHEN [Last Touch Channel Detail]	LIKE '%inflr%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'

			WHEN [Last Touch Channel Detail]	LIKE '%bing%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Bing'

				WHEN [Last Touch Channel Detail]	LIKE '%leadmedia%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'

				WHEN [Last Touch Channel Detail]	LIKE '%optimise%'		AND [Last Touch Channel Detail]	LIKE '%[_]cons[_]%' AND [Last Touch Channel Detail]	NOT LIKE '%fgts%'
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

		ELSE 'Outro' END AS CAMPANHA_LTCD_CONSIGNADO,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%awa%'		THEN 'Awareness'
			WHEN [Last Touch Channel Detail]	LIKE '%:cons[_]%'	THEN 'Consideração'
			WHEN [Last Touch Channel Detail]	LIKE '%perf%'		THEN 'Performance'
			WHEN [Last Touch Channel Detail]	LIKE '%engaj%'		THEN 'Engajamento'
		ELSE 'Outro' END AS OBJETIVO_CAMPANHA_LTCD_CONSIGNADO,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%mar-aberto%'													THEN 'Mar aberto'
			WHEN [Last Touch Channel Detail]	LIKE '%interesse%'													THEN 'Interesse'
			WHEN [Last Touch Channel Detail]	LIKE '%base-3party%'												THEN 'Base-3party'
			WHEN [Last Touch Channel Detail]	LIKE '%lal%'														THEN 'LAL'
			WHEN [Last Touch Channel Detail]	LIKE '%rmkt%' OR [Last Touch Channel Detail] LIKE '%remarketing%'	THEN 'Remarketing'
			WHEN [Last Touch Channel Detail]	LIKE '%base-1party%' OR [Last Touch Channel Detail]	LIKE '%base%'	THEN 'Base-1party'
		ELSE 'Outras' END AS SEGMENTACAO_LTCD_CONSIGNADO,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%lp-inss%'		THEN 'LP INSS'
			WHEN [Last Touch Channel Detail]	LIKE '%lp-siape%'		THEN 'LP SIAPE'
			WHEN [Last Touch Channel Detail]	LIKE '%lp-exercito%'	THEN 'LP EXÉRCITO'
			WHEN [Last Touch Channel Detail]	LIKE '%pdp-site%'		THEN 'PDP SITE'
			WHEN [Last Touch Channel Detail]	LIKE '%lp-fgts%'		THEN 'LP FGTS'
			WHEN [Last Touch Channel Detail]	LIKE '%funil-convenio%' THEN 'FUNIL CONVÊNIO'
			WHEN [Last Touch Channel Detail]	LIKE '%funil-ident%'	THEN 'FUNIL IDENT'
			WHEN [Last Touch Channel Detail]	LIKE '%whatsapp%'		THEN 'WHATSAPP'
		ELSE 'Outras' END AS URL_DESTINO_LTCD_CONSIGNADO,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%c11:%'	OR [Last Touch Channel Detail] LIKE '%c16:%' OR [Last Touch Channel Detail] LIKE '%c17:%' THEN 'Afiliados'
			WHEN [Last Touch Channel Detail]	LIKE '%c08:%'	OR [Last Touch Channel Detail] LIKE '%c09:%' OR [Last Touch Channel Detail] LIKE '%:c21%' THEN 'CRM'
			WHEN [Last Touch Channel Detail]	LIKE '%BP:%'	OR [Last Touch Channel Detail] LIKE '%None%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%https://www.bancopan.com.br%' AND [Last Touch Channel Detail] NOT LIKE '%idcmp%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%:empty:%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta FGTS'

			WHEN ([Last Touch Channel Detail]	LIKE '%facebook%' OR [Last Touch Channel Detail] LIKE '%criteo%' OR [Last Touch Channel Detail]	LIKE '%verizon%'
				OR [Last Touch Channel Detail]	LIKE '%inflr%' OR [Last Touch Channel Detail] LIKE '%bing%') AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta FGTS'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND [Last Touch Channel Detail] NOT LIKE '%[_]ccdig[_]%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]conta-digital-%' AND [Last Touch Channel Detail]	NOT LIKE '%[_]cart[_]%'
				AND [Last Touch Channel Detail]	NOT LIKE '%[_]cartoes[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]cartoes-%'
				AND [Last Touch Channel Detail]	NOT LIKE '%[_]autocontratacao[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail] LIKE '%[_]autocontratacao[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]consignado-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail]  LIKE '%[_]ccdig[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]conta-digital-%' OR [Last Touch Channel Detail] LIKE '%[_]cart[_]%'
				OR [Last Touch Channel Detail]	LIKE '%[_]cartoes[_]%' OR [Last Touch Channel Detail]  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN ([Last Touch Channel Detail]	LIKE '%facebook%' OR [Last Touch Channel Detail] LIKE '%criteo%' OR [Last Touch Channel Detail]	LIKE '%verizon%'
				OR [Last Touch Channel Detail]	LIKE '%inflr%' OR [Last Touch Channel Detail] LIKE '%bing%' OR [Last Touch Channel Detail] LIKE '%optimise%'
				OR [Last Touch Channel Detail] LIKE '%leadmedia%' OR [Last Touch Channel Detail] LIKE '%globo%'
				OR [Last Touch Channel Detail] LIKE '%UOL%' OR [Last Touch Channel Detail] LIKE '%tiktok%' OR [Last Touch Channel Detail] LIKE '%shopback%'
				OR [Last Touch Channel Detail] LIKE '%sbt%') AND [Last Touch Channel Detail] NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] LIKE '%midiaoff%' THEN 'Mídia Offline - FGTS'

		ELSE 'Outro' END AS TIPO_MIDIA_LTCD_FGTS, 

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%c11:%' THEN 'Afiliados - Outros produtos'
			WHEN [Last Touch Channel Detail]	LIKE '%c16:%' THEN 'Afiliados PF'
			WHEN [Last Touch Channel Detail]	LIKE '%c17:%' THEN 'Afiliados PJ'
			WHEN [Last Touch Channel Detail]	LIKE '%c08:%'	OR [Last Touch Channel Detail] LIKE '%c09:%' OR [Last Touch Channel Detail] LIKE '%:c21%' THEN 'CRM'
			WHEN [Last Touch Channel Detail]	LIKE '%BP:%'	OR [Last Touch Channel Detail] LIKE '%None%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%https://www.bancopan.com.br%' AND [Last Touch Channel Detail] NOT LIKE '%idcmp%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%:empty:%' THEN 'Orgânico + Direto'
			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Facebook'
			WHEN [Last Touch Channel Detail]	LIKE '%criteo%'		AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN [Last Touch Channel Detail]	LIKE '%verizon%'	AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Last Touch Channel Detail]	LIKE '%inflr%'		AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Last Touch Channel Detail]	LIKE '%bing%'		AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Last Touch Channel Detail]	LIKE '%optimise%'		AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Last Touch Channel Detail]	LIKE '%leadmedia%'		AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'
			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND [Last Touch Channel Detail] NOT LIKE '%[_]ccdig[_]%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]conta-digital-%' AND [Last Touch Channel Detail]	NOT LIKE '%[_]cart[_]%'
				AND [Last Touch Channel Detail]	NOT LIKE '%[_]cartoes[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]cartoes-%'
				AND [Last Touch Channel Detail]	NOT LIKE '%[_]autocontratacao[_]%' AND [Last Touch Channel Detail] NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail] LIKE '%[_]autocontratacao[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]consignado-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail]  LIKE '%[_]ccdig[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]conta-digital-%' OR [Last Touch Channel Detail] LIKE '%[_]cart[_]%'
				OR [Last Touch Channel Detail]	LIKE '%[_]cartoes[_]%' OR [Last Touch Channel Detail]  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN ([Last Touch Channel Detail]	LIKE '%facebook%' OR [Last Touch Channel Detail] LIKE '%criteo%' OR [Last Touch Channel Detail]	LIKE '%verizon%'
				OR [Last Touch Channel Detail]	LIKE '%inflr%' OR [Last Touch Channel Detail] LIKE '%bing%' OR [Last Touch Channel Detail] LIKE '%leadmedia%'
				OR [Last Touch Channel Detail] LIKE '%optimise%' OR [Last Touch Channel Detail] LIKE '%globo%'
				OR [Last Touch Channel Detail] LIKE '%UOL%' OR [Last Touch Channel Detail] LIKE '%tiktok%' OR [Last Touch Channel Detail] LIKE '%shopback%'
				OR [Last Touch Channel Detail] LIKE '%sbt%') AND [Last Touch Channel Detail] NOT LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] LIKE '%midiaoff%' AND [Last Touch Channel Detail] LIKE '%metro%' THEN 'Mídia Offline - Metrô'

		ELSE 'Outro' END AS ORIGEM_MIDIA_LTCD_FGTS,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail]  LIKE '%[_]ccdig[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]conta-digital-%' OR [Last Touch Channel Detail] LIKE '%[_]cart[_]%'
				OR [Last Touch Channel Detail]	LIKE '%[_]cartoes[_]%' OR [Last Touch Channel Detail]  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') 
				AND ([Last Touch Channel Detail] LIKE '%[_]autocontratacao[_]%' OR [Last Touch Channel Detail] LIKE '%[_]consignado-%') THEN 'Outro'	

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%inst%' THEN 'Google Search - Conta Inst'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Search'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%display%'
				AND [Last Touch Channel Detail] NOT LIKE '%discovery%' AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%disp%'
				AND [Last Touch Channel Detail] NOT LIKE '%discovery%' AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%discovery%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%gsp%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%[_]vid%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Geral'
			WHEN [Last Touch Channel Detail]	LIKE '%criteo%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN [Last Touch Channel Detail]	LIKE '%verizon%'	AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Last Touch Channel Detail]	LIKE '%inflr%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Last Touch Channel Detail]	LIKE '%bing%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Last Touch Channel Detail]	LIKE '%leadmedia%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Last Touch Channel Detail]	LIKE '%optimise%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'
		ELSE 'Outro' END AS TIPO_CAMPANHA_LTCD_FGTS,

		CASE
			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') AND ([Last Touch Channel Detail]  LIKE '%[_]ccdig[_]%' 
				OR [Last Touch Channel Detail]  LIKE '%[_]conta-digital-%' OR [Last Touch Channel Detail] LIKE '%[_]cart[_]%'
				OR [Last Touch Channel Detail]	LIKE '%[_]cartoes[_]%' OR [Last Touch Channel Detail]  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND ([Last Touch Channel Detail] LIKE '%inst%' OR [Last Touch Channel Detail] LIKE '%brand%') 
				AND ([Last Touch Channel Detail] LIKE '%[_]autocontratacao[_]%' OR [Last Touch Channel Detail] LIKE '%[_]consignado-%') THEN 'Outro'	
		
			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%inst%' AND ([Last Touch Channel Detail] LIKE '%institucional%' 
				OR [Last Touch Channel Detail] LIKE '%panamericano%') THEN 'Search Institucional - Inst'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%inst%' AND [Last Touch Channel Detail] LIKE '%atendimento%' THEN 'Search Institucional - Atendimento'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%inst%' THEN 'Search Institucional - Outras Campanhas' -- o que sobrar de "inst" cai em outras campanhas

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' AND ([Last Touch Channel Detail] LIKE '%marca%' 
				OR [Last Touch Channel Detail] LIKE '%produto-marca%') THEN 'Search FGTS - Produto + Marca'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' AND ([Last Touch Channel Detail] LIKE '%genericas%' 
				OR [Last Touch Channel Detail] LIKE '%generica%') THEN 'Search FGTS - Genéricas'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' AND ([Last Touch Channel Detail] LIKE '%concorrentes%' 
			OR [Last Touch Channel Detail] LIKE '%concorrente%') THEN 'Search FGTS - Concorrentes'	

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%srch%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Search FGTS - Outros'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%display%'
				AND [Last Touch Channel Detail] NOT LIKE '%discovery%' AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%disp%'
				AND [Last Touch Channel Detail] NOT LIKE '%discovery%' AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%discovery%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%gsp%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN [Last Touch Channel Detail]	LIKE '%google%' AND [Last Touch Channel Detail] NOT LIKE '%:empty%' AND [Last Touch Channel Detail] LIKE '%[_]vid%'
				AND [Last Touch Channel Detail] LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%all-placements%' AND [Last Touch Channel Detail]	LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - All Placements'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%post-feed%' AND [Last Touch Channel Detail]		LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Post feed'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%stories%' AND [Last Touch Channel Detail]		LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Stories/Reels'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%carrossel%' AND [Last Touch Channel Detail]		LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Carrossel'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%lead-ad%' AND [Last Touch Channel Detail]		LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Lead Ad'

			WHEN [Last Touch Channel Detail]	LIKE '%facebook%'	AND [Last Touch Channel Detail]	LIKE '%click-message%' AND [Last Touch Channel Detail]	LIKE '%fgts%' 
				AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'FB - Click Message'

			WHEN [Last Touch Channel Detail]	LIKE '%criteo%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Criteo - Retargeting'
			WHEN [Last Touch Channel Detail]	LIKE '%verizon%'	AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN [Last Touch Channel Detail]	LIKE '%inflr%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN [Last Touch Channel Detail]	LIKE '%bing%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN [Last Touch Channel Detail]	LIKE '%leadmedia%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN [Last Touch Channel Detail]	LIKE '%optimise%'		AND [Last Touch Channel Detail]	LIKE '%fgts%' AND [Last Touch Channel Detail] NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

		ELSE 'Outro' END AS CAMPANHA_LTCD_FGTS,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%awa%'		THEN 'Awareness'
			WHEN [Last Touch Channel Detail]	LIKE '%:cons[_]%'	THEN 'Consideração'
			WHEN [Last Touch Channel Detail]	LIKE '%perf%'		THEN 'Performance'
			WHEN [Last Touch Channel Detail]	LIKE '%engaj%'		THEN 'Engajamento'
		ELSE 'Outro' END AS OBJETIVO_CAMPANHA_LTCD_FGTS,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%mar-aberto%'													THEN 'Mar aberto'
			WHEN [Last Touch Channel Detail]	LIKE '%interesse%'													THEN 'Interesse'
			WHEN [Last Touch Channel Detail]	LIKE '%base-3party%'												THEN 'Base-3party'
			WHEN [Last Touch Channel Detail]	LIKE '%lal%'														THEN 'LAL'
			WHEN [Last Touch Channel Detail]	LIKE '%rmkt%' OR [Last Touch Channel Detail] LIKE '%remarketing%'	THEN 'Remarketing'
			WHEN [Last Touch Channel Detail]	LIKE '%base-1party%' OR [Last Touch Channel Detail]	LIKE '%base%'	THEN 'Base-1party'
		ELSE 'Outras' END AS SEGMENTACAO_LTCD_FGTS,

		CASE 
			WHEN [Last Touch Channel Detail]	LIKE '%lp-inss%'						THEN 'LP INSS'
			WHEN [Last Touch Channel Detail]	LIKE '%lp-siape%'						THEN 'LP SIAPE'
			WHEN [Last Touch Channel Detail]	LIKE '%lp-exercito%'					THEN 'LP EXÉRCITO'
			WHEN [Last Touch Channel Detail]	LIKE '%pdp-site%'						THEN 'PDP SITE'
			WHEN [Last Touch Channel Detail]	LIKE '%lp-fgts%'						THEN 'LP FGTS'
			WHEN [Last Touch Channel Detail]	LIKE '%funil-convenio%'					THEN 'FUNIL CONVÊNIO'
			WHEN [Last Touch Channel Detail]	LIKE '%funil-ident%'					THEN 'FUNIL IDENT'
			WHEN [Last Touch Channel Detail]	LIKE '%whatsapp%'						THEN 'WHATSAPP'
		ELSE 'Outras' END AS URL_DESTINO_LTCD_FGTS

INTO ##tb_DA_Consignado_Adobe_LTCD_Atual

FROM

(SELECT * FROM ##LTCD_DISTINTO) AS A

-- 5º) Consolidar os dados de LTCD e TC dos produtos (00:00:20)

DROP TABLE IF EXISTS ##tb_DA_Consignado_Adobe_Atual	

SELECT	CONVERT(DATE,A.Date) AS DATA_ADOBE,
		A.[Last touch channel detail] AS LAST_TOUCH_CHANNEL_DETAIL,
		A.[Tracking Code] AS TRACKING_CODE,
		CASE WHEN ISNUMERIC(A.[Global - ID CLI (v48) (evar48)]) = 0 THEN NULL ELSE A.[Global - ID CLI (v48) (evar48)] END AS ID_CLI,
		'Visitas Consignado' AS SEGMENTO,
		LTCD.TIPO_MIDIA_LTCD_CONSIGNADO,
		LTCD.ORIGEM_MIDIA_LTCD_CONSIGNADO,
		LTCD.TIPO_CAMPANHA_LTCD_CONSIGNADO,
		LTCD.CAMPANHA_LTCD_CONSIGNADO,
		LTCD.OBJETIVO_CAMPANHA_LTCD_CONSIGNADO,
		LTCD.SEGMENTACAO_LTCD_CONSIGNADO,
		LTCD.URL_DESTINO_LTCD_CONSIGNADO,
		LTCD.TIPO_MIDIA_LTCD_FGTS,
		LTCD.ORIGEM_MIDIA_LTCD_FGTS,
		LTCD.TIPO_CAMPANHA_LTCD_FGTS,
		LTCD.CAMPANHA_LTCD_FGTS,
		LTCD.OBJETIVO_CAMPANHA_LTCD_FGTS,
		LTCD.SEGMENTACAO_LTCD_FGTS,
		LTCD.URL_DESTINO_LTCD_FGTS,
		TC.TIPO_MIDIA_TC_CONSIGNADO,
		TC.ORIGEM_MIDIA_TC_CONSIGNADO,
		TC.TIPO_CAMPANHA_TC_CONSIGNADO,
		TC.CAMPANHA_TC_CONSIGNADO,
		TC.OBJETIVO_CAMPANHA_TC_CONSIGNADO,
		TC.SEGMENTACAO_TC_CONSIGNADO,
		TC.URL_DESTINO_TC_CONSIGNADO,
		TC.TIPO_MIDIA_TC_FGTS,
		TC.ORIGEM_MIDIA_TC_FGTS,
		TC.TIPO_CAMPANHA_TC_FGTS,
		TC.CAMPANHA_TC_FGTS,
		TC.OBJETIVO_CAMPANHA_TC_FGTS,
		TC.SEGMENTACAO_TC_FGTS,
		TC.URL_DESTINO_TC_FGTS

INTO ##tb_DA_Consignado_Adobe_Atual

FROM bi..tb_DA_Consignado_Adobe AS A WITH (NOLOCK)

	LEFT JOIN ##tb_DA_Consignado_Adobe_LTCD_Atual AS LTCD WITH (NOLOCK)
		ON ISNULL(A.[Last Touch Channel Detail],'NULL') = ISNULL(LTCD.[Last Touch Channel Detail],'NULL')

	LEFT JOIN ##tb_DA_Consignado_Adobe_TC_Atual AS TC WITH (NOLOCK)
		ON ISNULL(A.[Tracking Code],'NULL') = ISNULL(TC.[Tracking Code],'NULL')

-- 6º) Tratar as classificações da campanha priorizando as descrições do TC e, em caso null, considerar descrições do LTCD (00:00:10)

DROP TABLE IF EXISTS ##tb_DA_Consignado_Adobe_AtualTratado

SELECT	DATA_ADOBE,
		LAST_TOUCH_CHANNEL_DETAIL,
		TRACKING_CODE,
		ID_CLI,
		CASE WHEN TRACKING_CODE IS NULL THEN TIPO_MIDIA_LTCD_CONSIGNADO ELSE TIPO_MIDIA_TC_CONSIGNADO END						AS TIPO_MIDIA_CONSIGNADO,
		CASE WHEN TRACKING_CODE IS NULL THEN ORIGEM_MIDIA_LTCD_CONSIGNADO ELSE ORIGEM_MIDIA_TC_CONSIGNADO END					AS ORIGEM_MIDIA_CONSIGNADO,
		CASE WHEN TRACKING_CODE IS NULL THEN TIPO_CAMPANHA_LTCD_CONSIGNADO ELSE TIPO_CAMPANHA_TC_CONSIGNADO END					AS TIPO_CAMPANHA_CONSIGNADO,
		CASE WHEN TRACKING_CODE IS NULL THEN CAMPANHA_LTCD_CONSIGNADO ELSE CAMPANHA_TC_CONSIGNADO END							AS CAMPANHA_CONSIGNADO,
		CASE WHEN TRACKING_CODE IS NULL THEN OBJETIVO_CAMPANHA_LTCD_CONSIGNADO ELSE OBJETIVO_CAMPANHA_TC_CONSIGNADO END			AS OBJETIVO_CAMPANHA_CONSIGNADO,
		CASE WHEN TRACKING_CODE IS NULL THEN SEGMENTACAO_LTCD_CONSIGNADO ELSE SEGMENTACAO_TC_CONSIGNADO END						AS SEGMENTACAO_CONSIGNADO,
		CASE WHEN TRACKING_CODE IS NULL THEN URL_DESTINO_LTCD_CONSIGNADO ELSE URL_DESTINO_TC_CONSIGNADO END						AS URL_DESTINO_CONSIGNADO,
		CASE WHEN TRACKING_CODE IS NULL THEN TIPO_MIDIA_LTCD_FGTS ELSE TIPO_MIDIA_TC_FGTS END									AS TIPO_MIDIA_FGTS,
		CASE WHEN TRACKING_CODE IS NULL THEN ORIGEM_MIDIA_LTCD_FGTS ELSE ORIGEM_MIDIA_TC_FGTS END								AS ORIGEM_MIDIA_FGTS,
		CASE WHEN TRACKING_CODE IS NULL THEN TIPO_CAMPANHA_LTCD_FGTS ELSE TIPO_CAMPANHA_TC_FGTS END								AS TIPO_CAMPANHA_FGTS,
		CASE WHEN TRACKING_CODE IS NULL THEN CAMPANHA_LTCD_FGTS ELSE CAMPANHA_TC_FGTS END										AS CAMPANHA_FGTS,
		CASE WHEN TRACKING_CODE IS NULL THEN OBJETIVO_CAMPANHA_LTCD_FGTS ELSE OBJETIVO_CAMPANHA_TC_FGTS END						AS OBJETIVO_CAMPANHA_FGTS,
		CASE WHEN TRACKING_CODE IS NULL THEN SEGMENTACAO_LTCD_FGTS ELSE SEGMENTACAO_TC_FGTS END									AS SEGMENTACAO_FGTS,
		CASE WHEN TRACKING_CODE IS NULL THEN URL_DESTINO_LTCD_FGTS ELSE URL_DESTINO_TC_FGTS END									AS URL_DESTINO_FGTS,
		--CONVERT(VARCHAR(100),NULL) AS ClassificacaoResumoMidia,
		CONVERT(VARCHAR(500),NULL) AS NomeCampanha

INTO ##tb_DA_Consignado_Adobe_AtualTratado

FROM ##tb_DA_Consignado_Adobe_Atual

-- Atualizar 5x o campo "NomeCampanha" com os tratamentos de substring pelo campo do TC (casos em que taxonomia estiver ok) (00:08:00)

-- 1º UPDATE (remover tudo antes do 1º registro de ":" que aparecer no tracking code):
UPDATE ##tb_DA_Consignado_Adobe_AtualTratado
SET

NomeCampanha = SUBSTRING(tracking_code,CHARINDEX(':',tracking_code)+1,LEN(tracking_code))

-- 2º, 3º e 4º UPDATES (remover tudo antes do 2º,3º e 4º registros de ":" que aparecer no nome da campanha):
DECLARE @CONT INT
SET @CONT = 1

WHILE @CONT < 4

BEGIN

UPDATE ##tb_DA_Consignado_Adobe_AtualTratado
SET

NomeCampanha = SUBSTRING(NomeCampanha,CHARINDEX(':',NomeCampanha)+1,LEN(NomeCampanha))

SET @CONT = @CONT + 1

END

-- 5º UPDATE (manter tudo antes do próximo ":" que aparecer):

UPDATE ##tb_DA_Consignado_Adobe_AtualTratado
SET

NomeCampanha = SUBSTRING(NomeCampanha,0,CHARINDEX(':',NomeCampanha))

WHERE len (tracking_code) - len(replace(tracking_code,':','')) > 4 -- casos onde o padrão de taxonomia está correto e haverá um próximo ":"

-- Cruzamento entre propostas/digitações x infos de adobe
-- Retornar somente os matches entre cli id das tabelas + registros adobe com data até 30 dias atrás da data de entrada da proposta (00:01:00)

DROP TABLE IF EXISTS ##tb_DA_Consignado_PropostasB2C_Adobe_Atual

SELECT	convert(date,PROPOSTAS.dtEntrada) as DtEntrada,
		convert(date,convert(varchar(4),year(PROPOSTAS.dtEntrada)) + '-' 
			+ convert(varchar(2),month(PROPOSTAS.dtEntrada)) + '-' + '01')  as MesEntrada,
		PROPOSTAS.NuProposta,
		PROPOSTAS.NuCpf,
		PROPOSTAS.UFCliente,
		PROPOSTAS.StProposta,
		PROPOSTAS.VrProducao,
		PROPOSTAS.NmConvenio,
		PROPOSTAS.NmConvenioGrupo01,
		PROPOSTAS.DsTipoOperacaoGrupo02 AS DsTipoOperacao,
		PROPOSTAS.DsTipoCanalDeVenda,
		--PROPOSTAS.Operadora,
		PROPOSTAS.NmCorrespondente,
		PROPOSTAS.UFCorrespondente,
		PROPOSTAS.id_cliente_360 AS IDCliente360,
		ADOBE.DATA_ADOBE AS DtAdobe,
		convert(date,convert(varchar(4),year(ADOBE.DATA_ADOBE)) + '-' 
			+ convert(varchar(2),month(ADOBE.DATA_ADOBE)) + '-' + '01')  as MesAdobe,
		ADOBE.TRACKING_CODE AS TrackingCode,
		ADOBE.LAST_TOUCH_CHANNEL_DETAIL AS LastTouchChannelDetail,
		ADOBE.TIPO_MIDIA_CONSIGNADO AS TipoMidiaConsignado, -- Tratamento entre TC e LTCD
		ADOBE.ORIGEM_MIDIA_CONSIGNADO AS OrigemMidiaConsignado, -- Tratamento entre TC e LTCD
		ADOBE.TIPO_CAMPANHA_CONSIGNADO AS TipoCampanhaConsignado, -- Tratamento entre TC e LTCD
		ADOBE.CAMPANHA_CONSIGNADO AS CampanhaConsignado, -- Tratamento entre TC e LTCD
		ADOBE.OBJETIVO_CAMPANHA_CONSIGNADO AS ObjetivoCampanhaConsignado, -- Tratamento entre TC e LTCD
		ADOBE.SEGMENTACAO_CONSIGNADO AS SegmentacaoConsignado, -- Tratamento entre TC e LTCD
		ADOBE.URL_DESTINO_CONSIGNADO AS UrlDestinoConsignado, -- Tratamento entre TC e LTCD
		ADOBE.TIPO_MIDIA_FGTS AS TipoMidiaFGTS, -- Tratamento entre TC e LTCD
		ADOBE.ORIGEM_MIDIA_FGTS AS OrigemMidiaFgts, -- Tratamento entre TC e LTCD
		ADOBE.TIPO_CAMPANHA_FGTS AS TipoCampanhaFgts, -- Tratamento entre TC e LTCD
		ADOBE.CAMPANHA_FGTS AS CampanhaFgts, -- Tratamento entre TC e LTCD
		ADOBE.OBJETIVO_CAMPANHA_FGTS AS ObjetivoCampanhaFgts, -- Tratamento entre TC e LTCD
		ADOBE.SEGMENTACAO_FGTS AS SegmentacaoFgts, -- Tratamento entre TC e LTCD
		ADOBE.URL_DESTINO_FGTS AS UrlDestinoFgts, -- Tratamento entre TC e LTCD
		DATEDIFF(DAY,PROPOSTAS.dtEntrada,ADOBE.DATA_ADOBE) AS DiferencaDatas,
		CASE WHEN	DATEDIFF(DAY,PROPOSTAS.dtEntrada,ADOBE.DATA_ADOBE) <= 0 AND 
					DATEDIFF(DAY,PROPOSTAS.dtEntrada,ADOBE.DATA_ADOBE) >= -30 THEN 1 ELSE 0 END AS Flag30Dias,
		ROW_NUMBER() OVER(PARTITION BY PROPOSTAS.NuProposta ORDER BY ADOBE.DATA_ADOBE ASC) AS ID_AdobeGeral,
		CASE
			WHEN ADOBE.TIPO_MIDIA_CONSIGNADO = 'CRM' THEN ROW_NUMBER() OVER(PARTITION BY PROPOSTAS.NuProposta,ADOBE.TIPO_MIDIA_CONSIGNADO ORDER BY ADOBE.DATA_ADOBE ASC)
		ELSE NULL END AS ID_AdobeGeralCRM,
		-- ADOBE.ORIGEM_CAMPANHA <> 'CRM' THEN NULL ELSE ROW_NUMBER() OVER(PARTITION BY PROPOSTAS.NuProposta,ADOBE.ORIGEM_CAMPANHA ORDER BY ADOBE.DATA_ADOBE ASC) END AS ID_AdobeGeralCRM,
		--ADOBE.ClassificacaoResumoMidia,
		ADOBE.NomeCampanha,
		PROPOSTAS.DtContrato,
		PROPOSTAS.dtStProposta

INTO ##tb_DA_Consignado_PropostasB2C_Adobe_Atual

FROM ##tb_DA_Consignado_PropostasB2C_Atual AS PROPOSTAS

	LEFT JOIN ##tb_DA_Consignado_Adobe_AtualTratado AS ADOBE
		ON CONVERT(FLOAT,PROPOSTAS.id_cliente_360) = CONVERT(FLOAT,ADOBE.ID_CLI)

WHERE	CASE WHEN	DATEDIFF(DAY,PROPOSTAS.dtEntrada,ADOBE.DATA_ADOBE) <= 0 AND 
				DATEDIFF(DAY,PROPOSTAS.dtEntrada,ADOBE.DATA_ADOBE) >= -30 THEN 1 ELSE 0 END = 1

-- Realizar a atribuição do valor da proposta por cada ponto de contato que a proposta teve (00:00:10)

DROP TABLE IF EXISTS ##tb_DA_Consignado_PropostasB2C_AdobeAtribuido_Atual

SELECT	A.NuProposta,
		A.QdeContatos,
		B.VrProducao/A.QdeContatos AS VrAtribuicao,
		1/CONVERT(FLOAT,A.QdeContatos) AS QdeAtribuicao

INTO ##tb_DA_Consignado_PropostasB2C_AdobeAtribuido_Atual

FROM

(SELECT	NuProposta,
		count(nuProposta) as QdeContatos

FROM ##tb_DA_Consignado_PropostasB2C_Adobe_Atual

GROUP BY NuProposta

) AS A
	
	LEFT JOIN ##tb_DA_Consignado_PropostasB2C_Atual AS B
		ON A.NuProposta = B.NuProposta

-- Realizar o cruzamento da tabela de proposta x Adobe + o valor atribuido por cada ponto de contato identificado (00:00:10)

DROP TABLE IF EXISTS bi..tb_DA_Consignado_Propostas_Adobe_Geral

SELECT	A.*,
		B.VrAtribuicao,
		QdeAtribuicao,
		CASE
			WHEN TipoMidiaConsignado IN ('Orgânico + Direto','Outro')								THEN '01. Orgânico + Direto + Outros Canais'
			WHEN TipoMidiaConsignado = 'CRM'														THEN '02. CRM'
			WHEN TipoMidiaConsignado = 'Afiliados'													THEN '03. Afiliados'
			WHEN CampanhaConsignado = 'Search Institucional - Inst'									THEN '04. Search - Institucional - Inst'
			WHEN CampanhaConsignado = 'Search Institucional - Atendimento'							THEN '05. Search - Institucional - Atendimento'
			WHEN TipoMidiaConsignado = 'Mídia Paga - Outras Contas'									THEN '06. Mídia - Outras Contas'
			WHEN CampanhaConsignado = 'Search Consig - Produto + Marca'								THEN '07. Google Search - Produto + Marca'
			WHEN CampanhaConsignado = 'Search Consig - Genéricas'									THEN '08. Google Search - Genéricas'
			WHEN CampanhaConsignado = 'Search Consig - Concorrentes'								THEN '09. Google Search - Concorrentes'
			WHEN CampanhaConsignado = 'Search Consig - Outros'										THEN '10. Google Search - Outros'
			WHEN CampanhaConsignado = 'Google Display'												THEN '11. Google Display'
			WHEN CampanhaConsignado = 'Google Discovery'											THEN '12. Google Discovery'
			WHEN CampanhaConsignado = 'Google Vídeo'												THEN '13. Google Vídeo'
			WHEN OrigemMidiaConsignado = 'Facebook'													THEN '14. Facebook Ads'
			WHEN OrigemMidiaConsignado = 'Criteo'													THEN '15. Criteo'
			WHEN OrigemMidiaConsignado = 'Inflr'													THEN '16. Inflr'
			WHEN OrigemMidiaConsignado = 'Bing'														THEN '17. Bing'
			WHEN OrigemMidiaConsignado = 'LeadMedia'												THEN '19. LeadMedia'
			WHEN OrigemMidiaConsignado = 'Optimise'													THEN '20. Optimise'
		ELSE '18. Demais' END AS ClassificacaoResumoConsignado,
		CASE
			WHEN TipoMidiaFGTS IN ('Orgânico + Direto','Outro')										THEN '01. Orgânico + Direto + Outros Canais'
			WHEN TipoMidiaFGTS = 'CRM'																THEN '02. CRM'
			WHEN TipoMidiaFGTS = 'Afiliados'														THEN '03. Afiliados'
			WHEN CampanhaFgts = 'Search Institucional - Inst'										THEN '04. Search - Institucional - Inst'
			WHEN CampanhaFgts = 'Search Institucional - Atendimento'								THEN '05. Search - Institucional - Atendimento'
			WHEN TipoMidiaFGTS = 'Mídia Paga - Outras Contas'										THEN '06. Mídia - Outras Contas'
			WHEN CampanhaFgts = 'Search FGTS - Produto + Marca'										THEN '07. Google Search - Produto + Marca'
			WHEN CampanhaFgts = 'Search FGTS - Genéricas'											THEN '08. Google Search - Genéricas'
			WHEN CampanhaFgts = 'Search FGTS - Concorrentes'										THEN '09. Google Search - Concorrentes'
			WHEN CampanhaFgts = 'Search FGTS - Outros'												THEN '10. Google Search - Outros'
			WHEN CampanhaFgts = 'Google Display'													THEN '11. Google Display'
			WHEN CampanhaFgts = 'Google Discovery'													THEN '12. Google Discovery'
			WHEN CampanhaFgts = 'Google Vídeo'														THEN '13. Google Vídeo'
			WHEN OrigemMidiaFgts = 'Facebook'														THEN '14. Facebook Ads'
			WHEN OrigemMidiaFgts = 'Criteo'															THEN '15. Criteo'
			WHEN OrigemMidiaFgts = 'Inflr'															THEN '16. Inflr'
			WHEN OrigemMidiaFgts = 'Bing'															THEN '17. Bing'
			WHEN OrigemMidiaFgts = 'LeadMedia'														THEN '19. LeadMedia'
			WHEN OrigemMidiaFgts = 'Optimise'														THEN '20. Optimise'
		ELSE '18. Demais' END AS ClassificacaoResumoFGTS,
		CASE WHEN DsTipoOperacao = 'EP FGTS' THEN 'FGTS' ELSE 'Emprestimo Consignado' END AS Produto,
		convert(date,getdate()) as DtAtualizacao

INTO bi..tb_DA_Consignado_Propostas_Adobe_Geral

FROM ##tb_DA_Consignado_PropostasB2C_Adobe_Atual AS A

	LEFT JOIN ##tb_DA_Consignado_PropostasB2C_AdobeAtribuido_Atual AS B
		ON A.NuProposta = B.NuProposta

-- Tratamento dos dados de investimentos em consignado (dados vindo do Datorama, sem Inflr) (00:01:00)

DROP TABLE IF EXISTS bi..tb_DA_Investimentos_Geral

SELECT	CONVERT(DATE,Dia) as Dia,
		Nome_Campanha as NomeCampanha,
		Desc_Produto_Final,
		Tipo_Origem,
		Tipo,
		Conta,
		Cost,
		Impressions,
		Clicks,

		CASE WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta Consig'

			WHEN (Tipo	LIKE '%facebook%' OR Tipo LIKE '%criteo%' OR Tipo	LIKE '%verizon%'
				OR Tipo	LIKE '%inflr%' OR Tipo LIKE '%bing%') AND Nome_Campanha LIKE '%[_]cons[_]%' 
				AND Nome_Campanha NOT LIKE '%fgts%'AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta Consig'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') 
				AND (Nome_Campanha LIKE '%[_]autocontratacao[_]%' OR Nome_Campanha LIKE '%[_]consignado-%') THEN 'Mídia Paga - Conta Consig'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND Nome_Campanha NOT LIKE '%[_]ccdig[_]%' 
				AND Nome_Campanha NOT LIKE '%[_]conta-digital-%' AND Nome_Campanha	NOT LIKE '%[_]cart[_]%'
				AND Nome_Campanha	NOT LIKE '%[_]cartoes[_]%' AND Nome_Campanha NOT LIKE '%[_]cartoes-%' 
				AND Nome_Campanha NOT LIKE '%[_]autocontratacao[_]%' AND Nome_Campanha NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha  LIKE '%[_]ccdig[_]%' 
				OR Nome_Campanha  LIKE '%[_]conta-digital-%' OR Nome_Campanha LIKE '%[_]cart[_]%'
				OR Nome_Campanha	LIKE '%[_]cartoes[_]%' OR Nome_Campanha  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha NOT LIKE '%[_]cons[_]%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN (Tipo	LIKE '%facebook%' OR Tipo LIKE '%criteo%' OR Tipo	LIKE '%verizon%'
				OR Tipo	LIKE '%inflr%' OR Tipo LIKE '%bing%' OR Tipo LIKE '%leadmedia%' OR Tipo LIKE '%optimise%' 
				OR Tipo LIKE '%globo%' OR Tipo LIKE '%UOL%' OR Tipo LIKE '%tiktok%' OR Tipo LIKE '%shopback%'
				OR Tipo LIKE '%sbt%') AND Nome_Campanha NOT LIKE '%[_]cons[_]%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN Nome_Campanha	LIKE '%[_]cons[_]%'  AND Nome_Campanha NOT LIKE '%fgts%' AND Nome_Campanha LIKE '%midiaoff%' THEN 'Mídia Offline - Consig'

		ELSE 'Outro' END AS TIPO_MIDIA_CONSIGNADO, 

		CASE 
			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Facebook'
			WHEN Tipo	LIKE '%criteo%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN Tipo	LIKE '%verizon%'	AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN Tipo	LIKE '%inflr%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN Tipo	LIKE '%bing%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN Tipo	LIKE '%leadmedia%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN Tipo	LIKE '%optimise%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Optimise'


			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') 
				AND (Nome_Campanha LIKE '%[_]autocontratacao[_]%' OR Nome_Campanha LIKE '%[_]consignado-%') THEN 'Google'			

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND Nome_Campanha NOT LIKE '%[_]ccdig[_]%' 
				AND Nome_Campanha NOT LIKE '%[_]conta-digital-%' AND Nome_Campanha	NOT LIKE '%[_]cart[_]%'
				AND Nome_Campanha	NOT LIKE '%[_]cartoes[_]%' AND Nome_Campanha NOT LIKE '%[_]cartoes-%' 
				AND Nome_Campanha NOT LIKE '%[_]autocontratacao[_]%' AND Nome_Campanha NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha  LIKE '%[_]ccdig[_]%' 
				OR Nome_Campanha  LIKE '%[_]conta-digital-%' OR Nome_Campanha LIKE '%[_]cart[_]%'
				OR Nome_Campanha	LIKE '%[_]cartoes[_]%' OR Nome_Campanha  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha NOT LIKE '%[_]cons[_]%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN (Tipo	LIKE '%facebook%' OR Tipo LIKE '%criteo%' OR Tipo	LIKE '%verizon%'
				OR Tipo	LIKE '%inflr%' OR Tipo LIKE '%bing%' OR Tipo LIKE '%leadmedia%' OR Tipo LIKE '%optimise%' 
				OR Tipo LIKE '%globo%'
				OR Tipo LIKE '%UOL%' OR Tipo LIKE '%tiktok%' OR Tipo LIKE '%shopback%'
				OR Tipo LIKE '%sbt%') AND Nome_Campanha NOT LIKE '%[_]cons[_]%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN Nome_Campanha	LIKE '%[_]cons[_]%'  AND Nome_Campanha NOT LIKE '%fgts%' AND Nome_Campanha LIKE '%midiaoff%' AND Nome_Campanha LIKE '%metro%' THEN 'Mídia Offline - Metrô'

		ELSE 'Outro' END AS ORIGEM_MIDIA_CONSIGNADO,

		CASE 
			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha  LIKE '%[_]ccdig[_]%' 
				OR Nome_Campanha  LIKE '%[_]conta-digital-%' OR Nome_Campanha LIKE '%[_]cart[_]%'
				OR Nome_Campanha	LIKE '%[_]cartoes[_]%' OR Nome_Campanha  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') 
				AND (Nome_Campanha LIKE '%[_]autocontratacao[_]%' OR Nome_Campanha LIKE '%[_]consignado-%') THEN 'Google Search'	

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%inst%' THEN 'Google Search - Conta Inst'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Search'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%display%'
				AND Nome_Campanha NOT LIKE '%discovery%' AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%disp%'
				AND Nome_Campanha NOT LIKE '%discovery%' AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%discovery%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%gsp%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%[_]vid%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Geral'
			WHEN Tipo	LIKE '%criteo%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN Tipo	LIKE '%verizon%'	AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN Tipo	LIKE '%inflr%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN Tipo	LIKE '%bing%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN Tipo	LIKE '%leadmedia%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN Tipo	LIKE '%optimise%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

		ELSE 'Outro' END AS TIPO_CAMPANHA_CONSIGNADO,

		CASE
			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha  LIKE '%[_]ccdig[_]%' 
				OR Nome_Campanha  LIKE '%[_]conta-digital-%' OR Nome_Campanha LIKE '%[_]cart[_]%'
				OR Nome_Campanha	LIKE '%[_]cartoes[_]%' OR Nome_Campanha  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') 
				AND (Nome_Campanha LIKE '%[_]autocontratacao[_]%' OR Nome_Campanha LIKE '%[_]consignado-%') THEN 'Search Consig - Produto + Marca'	

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%inst%' AND (Nome_Campanha LIKE '%institucional%' 
				OR Nome_Campanha LIKE '%panamericano%') THEN 'Search Institucional - Inst'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%inst%' AND Nome_Campanha LIKE '%atendimento%' THEN 'Search Institucional - Atendimento'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%inst%' THEN 'Search Institucional - Outras Campanhas' -- o que sobrar de "inst" cai em outras campanhas

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' AND (Nome_Campanha LIKE '%marca%' 
				OR Nome_Campanha LIKE '%produto-marca%') THEN 'Search Consig - Produto + Marca'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' AND (Nome_Campanha LIKE '%genericas%' 
				OR Nome_Campanha LIKE '%generica%') THEN 'Search Consig - Genéricas'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' AND (Nome_Campanha LIKE '%concorrentes%' 
				OR Nome_Campanha LIKE '%concorrente%') THEN 'Search Consig - Concorrentes'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Search Consig - Outros'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%display%'
				AND Nome_Campanha NOT LIKE '%discovery%' AND Nome_Campanha LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%disp%'
				AND Nome_Campanha NOT LIKE '%discovery%' AND Nome_Campanha LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%discovery%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%gsp%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%[_]vid%'
				AND Nome_Campanha LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%all-placements%' AND Nome_Campanha	LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - All Placements'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%post-feed%' AND Nome_Campanha		LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Post feed'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%stories%' AND Nome_Campanha		LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Stories/Reels'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%carrossel%' AND Nome_Campanha		LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Carrossel'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%lead-ad%' AND Nome_Campanha		LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Lead Ad'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%click-message%' AND Nome_Campanha	LIKE '%[_]cons[_]%' 
				AND Nome_Campanha	NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Click Message'

			WHEN Tipo	LIKE '%criteo%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Criteo - Retargeting'

			WHEN Tipo	LIKE '%verizon%'	AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Verizon'

			WHEN Tipo	LIKE '%inflr%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Inflr'

			WHEN Tipo	LIKE '%bing%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Bing'

			WHEN Tipo	LIKE '%leadmedia%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'

			WHEN Tipo	LIKE '%optimise%'		AND Nome_Campanha	LIKE '%[_]cons[_]%' AND Nome_Campanha	NOT LIKE '%fgts%'
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

		ELSE 'Outro' END AS CAMPANHA_CONSIGNADO,

		CASE 
			WHEN Nome_Campanha	LIKE '%awa%'		THEN 'Awareness'
			WHEN Nome_Campanha	LIKE '%:cons[_]%'	THEN 'Consideração'
			WHEN Nome_Campanha	LIKE '%perf%'		THEN 'Performance'
			WHEN Nome_Campanha	LIKE '%engaj%'		THEN 'Engajamento'
		ELSE 'Outro' END AS OBJETIVO_CAMPANHA_CONSIGNADO,

		CASE 
			WHEN Nome_Campanha	LIKE '%mar-aberto%'										THEN 'Mar aberto'
			WHEN Nome_Campanha	LIKE '%interesse%'										THEN 'Interesse'
			WHEN Nome_Campanha	LIKE '%base-3party%'									THEN 'Base-3party'
			WHEN Nome_Campanha	LIKE '%lal%'											THEN 'LAL'
			WHEN Nome_Campanha	LIKE '%rmkt%' OR Nome_Campanha LIKE '%remarketing%'		THEN 'Remarketing'
			WHEN Nome_Campanha	LIKE '%base-1party%' OR Nome_Campanha	LIKE '%base%'	THEN 'Base-1party'
		ELSE 'Outras' END AS SEGMENTACAO_CONSIGNADO,

		CASE 
			WHEN Nome_Campanha	LIKE '%lp-inss%'				THEN 'LP INSS'
			WHEN Nome_Campanha	LIKE '%lp-siape%'				THEN 'LP SIAPE'
			WHEN Nome_Campanha	LIKE '%lp-exercito%'			THEN 'LP EXÉRCITO'
			WHEN Nome_Campanha	LIKE '%pdp-site%'				THEN 'PDP SITE'
			WHEN Nome_Campanha	LIKE '%lp-fgts%'				THEN 'LP FGTS'
			WHEN Nome_Campanha	LIKE '%funil-convenio%'			THEN 'FUNIL CONVÊNIO'
			WHEN Nome_Campanha	LIKE '%funil-ident%'			THEN 'FUNIL IDENT'
			WHEN Nome_Campanha	LIKE '%whatsapp%'				THEN 'WHATSAPP'
		ELSE 'Outras' END AS URL_DESTINO_CONSIGNADO,
		
		CASE 
			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta FGTS'

			WHEN (Tipo	LIKE '%facebook%' OR Tipo LIKE '%criteo%' OR Tipo	LIKE '%verizon%'
				OR Tipo	LIKE '%inflr%' OR Tipo LIKE '%bing%') AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Conta FGTS'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND Nome_Campanha NOT LIKE '%[_]ccdig[_]%' 
				AND Nome_Campanha NOT LIKE '%[_]conta-digital-%' AND Nome_Campanha	NOT LIKE '%[_]cart[_]%'
				AND Nome_Campanha	NOT LIKE '%[_]cartoes[_]%' AND Nome_Campanha NOT LIKE '%[_]cartoes-%'
				AND Nome_Campanha	NOT LIKE '%[_]autocontratacao[_]%' AND Nome_Campanha NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha LIKE '%[_]autocontratacao[_]%' 
				OR Nome_Campanha  LIKE '%[_]consignado-%') THEN 'Mídia Paga - Outras Contas'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha  LIKE '%[_]ccdig[_]%' 
				OR Nome_Campanha  LIKE '%[_]conta-digital-%' OR Nome_Campanha LIKE '%[_]cart[_]%'
				OR Nome_Campanha	LIKE '%[_]cartoes[_]%' OR Nome_Campanha  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN (Tipo	LIKE '%facebook%' OR Tipo LIKE '%criteo%' OR Tipo	LIKE '%verizon%'
				OR Tipo	LIKE '%inflr%' OR Tipo LIKE '%bing%' OR Tipo LIKE '%leadmedia%' OR Tipo LIKE '%optimise%' 
				OR Tipo LIKE '%globo%'
				OR Tipo LIKE '%UOL%' OR Tipo LIKE '%tiktok%' OR Tipo LIKE '%shopback%'
				OR Tipo LIKE '%sbt%') AND Nome_Campanha NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha LIKE '%midiaoff%' THEN 'Mídia Offline - FGTS'

		ELSE 'Outro' END AS TIPO_MIDIA_FGTS, 

		CASE 
			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Facebook'
			WHEN Tipo	LIKE '%criteo%'		AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN Tipo	LIKE '%verizon%'	AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN Tipo	LIKE '%inflr%'		AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN Tipo	LIKE '%bing%'		AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN Tipo	LIKE '%leadmedia%'		AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN Tipo	LIKE '%optimise%'		AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Optimise'
			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND Nome_Campanha NOT LIKE '%[_]ccdig[_]%' 
				AND Nome_Campanha NOT LIKE '%[_]conta-digital-%' AND Nome_Campanha	NOT LIKE '%[_]cart[_]%'
				AND Nome_Campanha	NOT LIKE '%[_]cartoes[_]%' AND Nome_Campanha NOT LIKE '%[_]cartoes-%'
				AND Nome_Campanha	NOT LIKE '%[_]autocontratacao[_]%' AND Nome_Campanha NOT LIKE '%[_]consignado-%' THEN 'Mídia Paga - Conta Inst'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha LIKE '%[_]autocontratacao[_]%' 
				OR Nome_Campanha  LIKE '%[_]consignado-%') THEN 'Mídia Paga - Outras Contas'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha  LIKE '%[_]ccdig[_]%' 
				OR Nome_Campanha  LIKE '%[_]conta-digital-%' OR Nome_Campanha LIKE '%[_]cart[_]%'
				OR Nome_Campanha	LIKE '%[_]cartoes[_]%' OR Nome_Campanha  LIKE '%[_]cartoes-%') THEN 'Mídia Paga - Outras Contas'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN (Tipo	LIKE '%facebook%' OR Tipo LIKE '%criteo%' OR Tipo	LIKE '%verizon%'
				OR Tipo	LIKE '%inflr%' OR Tipo LIKE '%bing%' OR Tipo LIKE '%leadmedia%' OR Tipo LIKE '%optimise%'
				OR Tipo LIKE '%globo%'
				OR Tipo LIKE '%UOL%' OR Tipo LIKE '%tiktok%' OR Tipo LIKE '%shopback%'
				OR Tipo LIKE '%sbt%') AND Nome_Campanha NOT LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Mídia Paga - Outras Contas'

			WHEN Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha LIKE '%midiaoff%' AND Nome_Campanha LIKE '%metro%' THEN 'Mídia Offline - Metrô'

		ELSE 'Outro' END AS ORIGEM_MIDIA_FGTS,

		CASE 
			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha  LIKE '%[_]ccdig[_]%' 
				OR Nome_Campanha  LIKE '%[_]conta-digital-%' OR Nome_Campanha LIKE '%[_]cart[_]%'
				OR Nome_Campanha	LIKE '%[_]cartoes[_]%' OR Nome_Campanha  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') 
				AND (Nome_Campanha LIKE '%[_]autocontratacao[_]%' OR Nome_Campanha LIKE '%[_]consignado-%') THEN 'Outro'	

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%inst%' THEN 'Google Search - Conta Inst'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Search'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%display%'
				AND Nome_Campanha NOT LIKE '%discovery%' AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%disp%'
				AND Nome_Campanha NOT LIKE '%discovery%' AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%discovery%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%gsp%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%[_]vid%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Geral'
			WHEN Tipo	LIKE '%criteo%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Criteo'
			WHEN Tipo	LIKE '%verizon%'	AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN Tipo	LIKE '%inflr%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN Tipo	LIKE '%bing%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN Tipo	LIKE '%leadmedia%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN Tipo	LIKE '%optimise%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

		ELSE 'Outro' END AS TIPO_CAMPANHA_FGTS,

		CASE
			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') AND (Nome_Campanha  LIKE '%[_]ccdig[_]%' 
				OR Nome_Campanha  LIKE '%[_]conta-digital-%' OR Nome_Campanha LIKE '%[_]cart[_]%'
				OR Nome_Campanha	LIKE '%[_]cartoes[_]%' OR Nome_Campanha  LIKE '%[_]cartoes-%') THEN 'Outro'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND (Nome_Campanha LIKE '%inst%' OR Nome_Campanha LIKE '%brand%') 
				AND (Nome_Campanha LIKE '%[_]autocontratacao[_]%' OR Nome_Campanha LIKE '%[_]consignado-%') THEN 'Outro'	
		
			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%inst%' AND (Nome_Campanha LIKE '%institucional%' 
				OR Nome_Campanha LIKE '%panamericano%') THEN 'Search Institucional - Inst'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%inst%' AND Nome_Campanha LIKE '%atendimento%' THEN 'Search Institucional - Atendimento'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%inst%' THEN 'Search Institucional - Outras Campanhas' -- o que sobrar de "inst" cai em outras campanhas

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' AND (Nome_Campanha LIKE '%marca%' 
				OR Nome_Campanha LIKE '%produto-marca%') THEN 'Search FGTS - Produto + Marca'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' AND (Nome_Campanha LIKE '%genericas%' 
				OR Nome_Campanha LIKE '%generica%') THEN 'Search FGTS - Genéricas'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' AND (Nome_Campanha LIKE '%concorrentes%' 
			OR Nome_Campanha LIKE '%concorrente%') THEN 'Search FGTS - Concorrentes'	

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%srch%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Search FGTS - Outros'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%display%'
				AND Nome_Campanha NOT LIKE '%discovery%' AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%disp%'
				AND Nome_Campanha NOT LIKE '%discovery%' AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Display'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%discovery%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%gsp%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Discovery'

			WHEN Tipo	LIKE '%google%' AND Nome_Campanha NOT LIKE '%:empty%' AND Nome_Campanha LIKE '%[_]vid%'
				AND Nome_Campanha LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Google Vídeo'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%all-placements%' AND Nome_Campanha	LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - All Placements'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%post-feed%' AND Nome_Campanha		LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Post feed'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%stories%' AND Nome_Campanha		LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Stories/Reels'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%carrossel%' AND Nome_Campanha		LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Carrossel'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%lead-ad%' AND Nome_Campanha		LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Lead Ad'

			WHEN Tipo	LIKE '%facebook%'	AND Nome_Campanha	LIKE '%click-message%' AND Nome_Campanha	LIKE '%fgts%' 
				AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'FB - Click Message'

			WHEN Tipo	LIKE '%criteo%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Criteo - Retargeting'
			WHEN Tipo	LIKE '%verizon%'	AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Verizon'
			WHEN Tipo	LIKE '%inflr%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Inflr'
			WHEN Tipo	LIKE '%bing%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Bing'
			WHEN Tipo	LIKE '%leadmedia%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'LeadMedia'
			WHEN Tipo	LIKE '%optimise%'		AND Nome_Campanha	LIKE '%fgts%' AND Nome_Campanha NOT LIKE '%[_]inst[_]%' THEN 'Optimise'

		ELSE 'Outro' END AS CAMPANHA_FGTS,

		CASE 
			WHEN Nome_Campanha	LIKE '%awa%'		THEN 'Awareness'
			WHEN Nome_Campanha	LIKE '%:cons[_]%'	THEN 'Consideração'
			WHEN Nome_Campanha	LIKE '%perf%'		THEN 'Performance'
			WHEN Nome_Campanha	LIKE '%engaj%'		THEN 'Engajamento'
		ELSE 'Outro' END AS OBJETIVO_CAMPANHA_FGTS,

		CASE 
			WHEN Nome_Campanha	LIKE '%mar-aberto%'										THEN 'Mar aberto'
			WHEN Nome_Campanha	LIKE '%interesse%'										THEN 'Interesse'
			WHEN Nome_Campanha	LIKE '%base-3party%'									THEN 'Base-3party'
			WHEN Nome_Campanha	LIKE '%lal%'											THEN 'LAL'
			WHEN Nome_Campanha	LIKE '%rmkt%' OR Nome_Campanha LIKE '%remarketing%'		THEN 'Remarketing'
			WHEN Nome_Campanha	LIKE '%base-1party%' OR Nome_Campanha	LIKE '%base%'	THEN 'Base-1party'
		ELSE 'Outras' END AS SEGMENTACAO_FGTS,

		CASE 
			WHEN Nome_Campanha	LIKE '%lp-inss%'			THEN 'LP INSS'
			WHEN Nome_Campanha	LIKE '%lp-siape%'			THEN 'LP SIAPE'
			WHEN Nome_Campanha	LIKE '%lp-exercito%'		THEN 'LP EXÉRCITO'
			WHEN Nome_Campanha	LIKE '%pdp-site%'			THEN 'PDP SITE'
			WHEN Nome_Campanha	LIKE '%lp-fgts%'			THEN 'LP FGTS'
			WHEN Nome_Campanha	LIKE '%funil-convenio%'		THEN 'FUNIL CONVÊNIO'
			WHEN Nome_Campanha	LIKE '%funil-ident%'		THEN 'FUNIL IDENT'
			WHEN Nome_Campanha	LIKE '%whatsapp%'			THEN 'WHATSAPP'
		ELSE 'Outras' END AS URL_DESTINO_FGTS,
		CONVERT(VARCHAR(100),NULL) COLLATE Latin1_General_CI_AS AS ClassificacaoResumoConsignado,
		CONVERT(VARCHAR(100),NULL) COLLATE Latin1_General_CI_AS AS ClassificacaoResumoFGTS,
		convert(date,getdate()) as DtAtualizacao

INTO bi..tb_DA_Investimentos_Geral

FROM bi..tb_DA_Investimentos_Datorama WITH (NOLOCK)

WHERE	Dia >= (select min(dtadobe) from bi..tb_DA_Consignado_Propostas_Adobe_Geral) -- somente considerar datas de investimentos a partir da primeira data adobe da tabela de propostas
	AND Desc_Produto_Final IN ('Emprestimo Consignado','FGTS') -- Consignado + FGTS
	AND Conta NOT LIKE '%AFILIADOS%' -- não considerar contas de afiliados

-- Atualizar os campos "ClassificacaoResumoConsignado" e "ClassificacaoResumoFGTS" (00:00:05)

UPDATE bi..tb_DA_Investimentos_Geral
SET

ClassificacaoResumoConsignado = 		CASE
											WHEN TIPO_MIDIA_CONSIGNADO		IN ('Orgânico + Direto','Outro')								THEN '01. Orgânico + Direto + Outros Canais'
											WHEN TIPO_MIDIA_CONSIGNADO		= 'CRM'															THEN '02. CRM'
											WHEN TIPO_MIDIA_CONSIGNADO		= 'Afiliados'													THEN '03. Afiliados'
											WHEN CAMPANHA_CONSIGNADO		= 'Search Institucional - Inst'									THEN '04. Search - Institucional - Inst'
											WHEN CAMPANHA_CONSIGNADO		= 'Search Institucional - Atendimento'							THEN '05. Search - Institucional - Atendimento'
											WHEN TIPO_MIDIA_CONSIGNADO		= 'Mídia Paga - Outras Contas'									THEN '06. Mídia - Outras Contas'
											WHEN CAMPANHA_CONSIGNADO		= 'Search Consig - Produto + Marca'								THEN '07. Google Search - Produto + Marca'
											WHEN CAMPANHA_CONSIGNADO		= 'Search Consig - Genéricas'									THEN '08. Google Search - Genéricas'
											WHEN CAMPANHA_CONSIGNADO		= 'Search Consig - Concorrentes'								THEN '09. Google Search - Concorrentes'
											WHEN CAMPANHA_CONSIGNADO		= 'Search Consig - Outros'										THEN '10. Google Search - Outros'
											WHEN CAMPANHA_CONSIGNADO		= 'Google Display'												THEN '11. Google Display'
											WHEN CAMPANHA_CONSIGNADO		= 'Google Discovery'											THEN '12. Google Discovery'
											WHEN CAMPANHA_CONSIGNADO		= 'Google Vídeo'												THEN '13. Google Vídeo'
											WHEN ORIGEM_MIDIA_CONSIGNADO	= 'Facebook'													THEN '14. Facebook Ads'
											WHEN ORIGEM_MIDIA_CONSIGNADO	= 'Criteo'														THEN '15. Criteo'
											WHEN ORIGEM_MIDIA_CONSIGNADO	= 'Inflr'														THEN '16. Inflr'
											WHEN ORIGEM_MIDIA_CONSIGNADO	= 'Bing'														THEN '17. Bing'
											WHEN ORIGEM_MIDIA_CONSIGNADO	= 'LeadMedia'													THEN '19. LeadMedia'
											WHEN ORIGEM_MIDIA_CONSIGNADO	= 'Optimise'													THEN '20. Optimise'
										ELSE '18. Demais' END,

ClassificacaoResumoFGTS = 				CASE
											WHEN TIPO_MIDIA_FGTS	IN ('Orgânico + Direto','Outro')										THEN '01. Orgânico + Direto + Outros Canais'
											WHEN TIPO_MIDIA_FGTS	= 'CRM'																	THEN '02. CRM'
											WHEN TIPO_MIDIA_FGTS	= 'Afiliados'															THEN '03. Afiliados'
											WHEN CAMPANHA_FGTS		= 'Search Institucional - Inst'											THEN '04. Search - Institucional - Inst'
											WHEN CAMPANHA_FGTS		= 'Search Institucional - Atendimento'									THEN '05. Search - Institucional - Atendimento'
											WHEN TIPO_MIDIA_FGTS	= 'Mídia Paga - Outras Contas'											THEN '06. Mídia - Outras Contas'
											WHEN CAMPANHA_FGTS		= 'Search FGTS - Produto + Marca'										THEN '07. Google Search - Produto + Marca'
											WHEN CAMPANHA_FGTS		= 'Search FGTS - Genéricas'												THEN '08. Google Search - Genéricas'
											WHEN CAMPANHA_FGTS		= 'Search FGTS - Concorrentes'											THEN '09. Google Search - Concorrentes'
											WHEN CAMPANHA_FGTS		= 'Search FGTS - Outros'												THEN '10. Google Search - Outros'
											WHEN CAMPANHA_FGTS		= 'Google Display'														THEN '11. Google Display'
											WHEN CAMPANHA_FGTS		= 'Google Discovery'													THEN '12. Google Discovery'
											WHEN CAMPANHA_FGTS		= 'Google Vídeo'														THEN '13. Google Vídeo'
											WHEN ORIGEM_MIDIA_FGTS	= 'Facebook'															THEN '14. Facebook Ads'
											WHEN ORIGEM_MIDIA_FGTS	= 'Criteo'																THEN '15. Criteo'
											WHEN ORIGEM_MIDIA_FGTS	= 'Inflr'																THEN '16. Inflr'
											WHEN ORIGEM_MIDIA_FGTS	= 'Bing'																THEN '17. Bing'
											WHEN ORIGEM_MIDIA_FGTS	= 'LeadMedia'															THEN '19. LeadMedia'
											WHEN ORIGEM_MIDIA_FGTS	= 'Optimise'															THEN '20. Optimise'
										ELSE '18. Demais' END

-- Realizar o rateio de 300k de Inflr de 26/04/2021 - 31/07/2021 para considerar na tabela de investimentos (00:00:05)

DECLARE @BUDGET_TOTAL_INFLR FLOAT, @BUDGET_DIARIO_INFLR FLOAT, @DIAS_INFLR FLOAT
DECLARE @DT_INICIO_INFLR DATE, @DT_FIM_INFLR DATE

SET @BUDGET_TOTAL_INFLR = 300000
SET @DT_INICIO_INFLR = '2021-04-26'
SET @DT_FIM_INFLR = '2021-08-01'
SET @DIAS_INFLR = DATEDIFF(DAY,@DT_INICIO_INFLR,@DT_FIM_INFLR)
SET @BUDGET_DIARIO_INFLR = @BUDGET_TOTAL_INFLR / @DIAS_INFLR

-- Criar tabela para considerar os dias do Inflr iguais aos dias dos outros investimento e data da adobe
DROP TABLE IF EXISTS ##tb_DA_Inflr_Investimentos

CREATE TABLE ##tb_DA_Inflr_Investimentos (	Dia datetime,
											NomeCampanha nvarchar(255),
											Desc_Produto_Final nvarchar(100),
											Tipo_Origem nvarchar(255),
											Tipo varchar(50),
											Conta nvarchar(255),
											Cost money,
											Impressions float,
											Clicks float,
											TIPO_MIDIA_CONSIGNADO varchar(26),
											ORIGEM_MIDIA_CONSIGNADO varchar(26),
											TIPO_CAMPANHA_CONSIGNADO varchar(26),
											CAMPANHA_CONSIGNADO varchar(39),
											OBJETIVO_CAMPANHA_CONSIGNADO varchar(12),
											SEGMENTACAO_CONSIGNADO varchar(11),
											URL_DESTINO_CONSIGNADO varchar(14),
											TIPO_MIDIA_FGTS varchar(26),
											ORIGEM_MIDIA_FGTS varchar(26),
											TIPO_CAMPANHA_FGTS varchar(26),
											CAMPANHA_FGTS varchar(39),
											OBJETIVO_CAMPANHA_FGTS varchar(12),
											SEGMENTACAO_FGTS varchar(11),
											URL_DESTINO_FGTS varchar(14),
											ClassificacaoResumoConsignado varchar(100),
											ClassificacaoResumoFGTS varchar(100),
											DtAtualizacao datetime
											);

-- Loop para inserção dos dados de Inflr baseado no critério das datas do adobe e fim da campanha
WHILE @DT_INICIO_INFLR < @DT_FIM_INFLR

BEGIN

INSERT INTO ##tb_DA_Inflr_Investimentos

SELECT	@DT_INICIO_INFLR as Dia,
		'Outras Campanhas' collate Latin1_General_CI_AS as NomeCampanha,
		'Emprestimo Consignado' as Desc_Produto_Final,
		'Inflr' as Tipo_Origem,
		'Inflr' as Tipo,
		'Inflr' as Conta,
		@BUDGET_DIARIO_INFLR as Cost,
		0 as Impressions,
		0 as Clicks,
		'Mídia Paga - Conta Consig' as TIPO_MIDIA_CONSIGNADO,
		'Inflr' as ORIGEM_MIDIA_CONSIGNADO,
		'Inflr' as TIPO_CAMPANHA_CONSIGNADO,
		'Inflr' as CAMPANHA_CONSIGNADO,
		'Outro' as OBJETIVO_CAMPANHA_CONSIGNADO,
		'Outro' as SEGMENTACAO_CONSIGNADO,
		'Outro' as URL_DESTINO_CONSIGNADO,
		'Mídia Paga - Outras Contas' as TIPO_MIDIA_FGTS,
		'Mídia Paga - Outras Contas' as ORIGEM_MIDIA_FGTS,
		'Outro' as TIPO_CAMPANHA_FGTS,
		'Outro' as CAMPANHA_FGTS,
		'Outro' as OBJETIVO_CAMPANHA_FGTS,
		'Outro' as SEGMENTACAO_FGTS,
		'Outro' as URL_DESTINO_FGTS,
		'16. Inflr' as ClassificacaoResumoConsignado,
		'06. Mídia - Outras Contas' as ClassificacaoResumoFGTS,
		convert(date,getdate()) as DtAtualizacao

SET @DT_INICIO_INFLR = DATEADD(DAY,1,@DT_INICIO_INFLR)

END

-- Juntar os dados de Inflr na tabela de investimentos
INSERT INTO bi..tb_DA_Investimentos_Geral
SELECT * FROM ##tb_DA_Inflr_Investimentos