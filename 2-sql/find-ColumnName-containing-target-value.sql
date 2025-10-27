/*
Single-table search: [DWH_Dyconex].[engineering].[ProcessSteps]
Goal: Identify which column(s) (excluding BuildUpLinkText) contain the substring 'S-FXM-1',
      restricting rows to ProcessNumber = '10402' OR ProcessID = 93 — but only if those columns exist.

Strategy:
  1) Detect whether columns [ProcessNumber] and/or [ProcessID] exist in the table.
  2) Build a row filter accordingly:
       - If both exist: (ProcessNumber='10402' OR ProcessID=93)
       - If only one exists: use the one that exists
       - If none exist: set filter to 1 = 0 (no scan)
  3) Enumerate character columns, excluding BuildUpLinkText.
  4) For each column, run a parameterized LIKE with the filter and insert matches into #Hits.
  5) No TRY_CONVERT / CAST; LIKE only on character columns.
*/

SET NOCOUNT ON;

DECLARE @SearchValue NVARCHAR(200) = N'S-FXM-1';

IF OBJECT_ID('tempdb..#Cols') IS NOT NULL DROP TABLE #Cols;
CREATE TABLE #Cols (ColumnName SYSNAME NOT NULL);

IF OBJECT_ID('tempdb..#Hits') IS NOT NULL DROP TABLE #Hits;
CREATE TABLE #Hits (
    ColumnName SYSNAME NOT NULL,
    MatchCount INT     NOT NULL
);

DECLARE @HasProcessNumber BIT = 0;
DECLARE @HasProcessID     BIT = 0;

-- Detect column existence
SELECT @HasProcessNumber = 1
FROM [DWH_Dyconex].sys.columns c
JOIN [DWH_Dyconex].sys.tables  t ON c.object_id = t.object_id
JOIN [DWH_Dyconex].sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = N'engineering' AND t.name = N'ProcessSteps' AND c.name = N'ProcessNumber';

SELECT @HasProcessID = 1
FROM [DWH_Dyconex].sys.columns c
JOIN [DWH_Dyconex].sys.tables  t ON c.object_id = t.object_id
JOIN [DWH_Dyconex].sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = N'engineering' AND t.name = N'ProcessSteps' AND c.name = N'ProcessID';

DECLARE @RowFilter NVARCHAR(1000);

IF (@HasProcessNumber = 1 AND @HasProcessID = 1)
    SET @RowFilter = N'(ProcessNumber = N''10402'' OR ProcessID = 93)';
ELSE IF (@HasProcessNumber = 1 AND @HasProcessID = 0)
    SET @RowFilter = N'(ProcessNumber = N''10402'')';
ELSE IF (@HasProcessNumber = 0 AND @HasProcessID = 1)
    SET @RowFilter = N'(ProcessID = 93)';
ELSE
    SET @RowFilter = N'1 = 0';  -- neither column exists; avoid full-table scan

-- 1) Candidate character columns (excluding BuildUpLinkText)
INSERT INTO #Cols (ColumnName)
SELECT c.name
FROM   [DWH_Dyconex].sys.tables  AS t
JOIN   [DWH_Dyconex].sys.schemas AS s  ON s.schema_id = t.schema_id
JOIN   [DWH_Dyconex].sys.columns AS c  ON c.object_id = t.object_id
JOIN   [DWH_Dyconex].sys.types   AS ty ON ty.user_type_id = c.user_type_id
WHERE  s.name = N'engineering'
  AND  t.name = N'ProcessSteps'
  AND  c.name <> N'BuildUpLinkText'
  AND  ty.name IN (N'char', N'nchar', N'varchar', N'nvarchar');  -- skip text/ntext

-- 2) Scan per column with direct LIKE and the dynamic row filter
DECLARE @Col SYSNAME;
DECLARE @sql NVARCHAR(MAX);

DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT ColumnName FROM #Cols;

OPEN col_cur;
FETCH NEXT FROM col_cur INTO @Col;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
IF EXISTS (
    SELECT 1
    FROM [DWH_Dyconex].[engineering].[ProcessSteps]
    WHERE ' + QUOTENAME(@Col) + N' LIKE N''%'' + @p + N''%''
      AND ' + @RowFilter + N'
)
BEGIN
    INSERT INTO #Hits (ColumnName, MatchCount)
    SELECT N''' + REPLACE(@Col, '''', '''''') + N''',
           COUNT(*)
    FROM [DWH_Dyconex].[engineering].[ProcessSteps]
    WHERE ' + QUOTENAME(@Col) + N' LIKE N''%'' + @p + N''%''
      AND ' + @RowFilter + N';
END';

    EXEC sp_executesql @sql, N'@p NVARCHAR(200)', @p = @SearchValue;

    FETCH NEXT FROM col_cur INTO @Col;
END

CLOSE col_cur;
DEALLOCATE col_cur;

-- 3) Results + diagnostics
SELECT ColumnName, MatchCount
FROM   #Hits
ORDER BY ColumnName;

SELECT 
    HasProcessNumber = @HasProcessNumber,
    HasProcessID     = @HasProcessID,
    EffectiveRowFilter = @RowFilter;
