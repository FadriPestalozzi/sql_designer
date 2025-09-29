USE DWH_Dyconex; --choose from available sys.databases
GO

SELECT
    t.name       AS TableName,
    i.name       AS PrimaryKeyName,
    c.name       AS ColumnName,
    ic.key_ordinal AS KeyOrder --KeyOrder = columns of composite primary keys
FROM sys.indexes i
INNER JOIN sys.index_columns ic
    ON i.object_id = ic.object_id
   AND i.index_id  = ic.index_id
INNER JOIN sys.columns c
    ON ic.object_id = c.object_id
   AND ic.column_id = c.column_id
INNER JOIN sys.tables t
    ON i.object_id = t.object_id
WHERE i.is_primary_key = 1
ORDER BY t.name, ic.key_ordinal; 
