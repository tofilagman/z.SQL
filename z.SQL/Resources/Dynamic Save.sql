
-- -------------------------
-- @LJGomez, 20160927
-- Dynamic Table
-- -> Save Info 
-- /* If You See this script and found errors, Need Modification or Improrvement, please refer to @Software Provider */
-- ------------------------

SET TRANSACTION ISOLATION LEVEL READ COMMITTED
SET NOCOUNT ON
SET XACT_ABORT ON

IF OBJECT_ID(N'tempdb.dbo.#STable') IS NOT NULL DROP TABLE #STable
IF OBJECT_ID(N'tempdb.dbo.#PTable') IS NOT NULL DROP TABLE #PTable
IF OBJECT_ID(N'tempdb.dbo.#DTable') IS NOT NULL DROP TABLE #DTable

CREATE TABLE #STable ({{SchemaTable}})
CREATE TABLE #PTable ({{SchemaTable}})
CREATE TABLE #DTable ([ID] INT)

{{InsertDataTable}}   --INSERT INTO #STable values {{ old.DataTable }} 
INSERT INTO #DTable VALUES {{DeletedTable}}

DECLARE @tbl AS TABLE(ID INT, RowType INT, RID INT IDENTITY(1, 1))

-- Generic 
-- Delete
INSERT INTO #PTable ([ID], {{SchemaInsert}})
	SELECT T.ID, {{SelectInsert}} FROM {{TableName}} AS T WITH(NOLOCK) WHERE T.ID IN (SELECT ID FROM #DTable)

DELETE
	FROM 
	{{TableName}} 
	OUTPUT Deleted.ID, 2 INTO @tbl 
	WHERE ID IN (SELECT ID FROM #DTable)

IF EXISTS(SELECT * FROM #STable WHERE ID > 0)
BEGIN
	INSERT INTO #PTable ([ID], {{SchemaInsert}})
	SELECT T.ID, {{SelectInsert}} FROM {{TableName}} AS T WITH(NOLOCK) INNER JOIN #STable AS S ON T.ID = S.ID WHERE S.ID > 0

	UPDATE T SET 
	{{SchemaUpdate}}
	FROM {{TableName}} AS T WITH(NOLOCK) INNER JOIN #STable AS S ON T.ID = S.ID WHERE S.ID > 0
END

IF EXISTS(SELECT * FROM #STable WHERE ID <= 0)
BEGIN
	IF 0 = {{HasIndex}}
	BEGIN 
		INSERT INTO {{TableName}}
		({{SchemaInsert}})
		OUTPUT Inserted.ID, 0 INTO @tbl
		SELECT {{ColumnInsert}} FROM #STable AS S WHERE S.ID <= 0 
	END
	ELSE
	BEGIN
		INSERT INTO {{TableName}}
		({{SchemaInsert}})
		OUTPUT Inserted.ID, 0 INTO @tbl
		SELECT {{ColumnInsert}} FROM #STable AS S 
		WHERE S.ID <= 0  AND NOT EXISTS(
			SELECT * FROM {{TableName}} T WITH(NOLOCK) WHERE {{WhereIndex}}
			)

		INSERT INTO @tbl 
		SELECT T.ID, 0 FROM {{TableName}} T WITH(NOLOCK)
			INNER JOIN #STable AS S ON {{WhereIndex}}
	END 
	-- update temp table for new values
	DELETE FROM #STable WHERE ID <= 0
END

INSERT INTO @tbl 
SELECT ID, 1 FROM #STable s
	WHERE NOT EXISTS(SELECT * FROM @tbl WHERE ID = s.ID) 

SELECT ID FROM @tbl

DROP TABLE #STable
DROP TABLE #PTable
DROP TABLE #DTable