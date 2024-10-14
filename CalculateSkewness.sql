SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Note: * Does not use Welford's numerically stable one-pass.
--         This should not be a problem in the majority of cases.
--
-- Can be run for all tables' columns:
--     exec dbo.CalculateSkewness NULL, NULL, NULL, 50, 0
--
-- or all tables' columns in a schema:
--     exec dbo.CalculateSkewness 'dbo', NULL, NULL, 50, 0
--
-- or all columns in a single table, 
--     exec dbo.CalculateSkewness 'dbo', 'MyTable', NULL, 50, 0
--
-- or just a single column.
--     exec dbo.CalculateSkewness 'dbo', 'MyTable', 'MyColumn', 50, 0
--
-- Might take a while on a large database.
-- Run at own risk
--
-- Adapted from:
-- https://gitlab.com/swasheck/statistics-scripts/blob/master/table%20analysis.sql
--
-- https://brownmath.com/stat/shape.htm
-- https://learnsql.com/blog/high-performance-statistical-queries-skewness-kurtosis/
-- https://swasheck.wordpress.com/2016/04/06/skewed-data-finding-the-columns/
-- 
CREATE OR ALTER PROCEDURE dbo.CalculateSkewness
(
    @pSchemaName sysname = NULL,
    @pTableName sysname = NULL,
    @pColumnName sysname = NULL,
    @SamplePercent int = -1, 
    @Debug bit = 1
)
AS
BEGIN

SET NOCOUNT ON;
SET XACT_ABORT ON;

-- Check samplePercent between 1 and 100 (or -1)
if @SamplePercent < 0 
    SET @SamplePercent = -1;
else 
    if @SamplePercent > 100 
        SET @SamplePercent = 100;

declare @TableRowCount bigint;
declare @tallysql nvarchar(max);
declare @sql nvarchar(max);
DECLARE @TallyTable SYSNAME;

drop table if exists #tmp_tally_table;
drop table if exists #table_analysis;

create table #tmp_tally_table
(
    [key] nvarchar(max),
    x decimal(30,0),
    f decimal(30,0)
);

create table #table_analysis 
(
	schema_name sysname, 
	table_name sysname, 
	column_name sysname,
	b1 decimal(30,5),
	G1 decimal(30,5),
	ses decimal(30,5),
	distinct_values decimal(30,0),
	total_values decimal(30,0),
	distinct_ratio decimal(20,10),
	table_rows decimal(30,0),
	distinct_ratio_null decimal(20,10),
	Zg1 decimal(30,5),
    tallysql nvarchar(max)
);

DECLARE @SchemaName sysname, @TableName sysname, @ColumnName sysname;

declare colcur cursor local fast_forward for
SELECT
    sc.name,
    t.name,
    c.name
FROM 
    sys.tables t
    JOIN sys.schemas sc ON sc.schema_id = t.schema_id
    JOIN sys.columns c ON c.object_id = t.object_id
    JOIN sys.types ty ON ty.user_type_id = c.user_type_id
WHERE
    sc.name != 'cdc' 
    AND sc.name != 'history'
    and t.name not like '#%' and t.name != 'awsdms_truncation_safeguard'
    and c.is_identity = 0
    and c.is_computed = 0
    and c.is_filestream = 0
    and c.is_xml_document = 0
    and t.is_memory_optimized = 0
    and ty.max_length != -1    -- skip (n)varchar(max) columns
    and ty.max_length < 200    -- skip long text columns
	AND OBJECTPROPERTY(t.object_id, 'IsTable') = 1 
    AND (@pSchemaName IS NULL OR sc.name = @pSchemaName)
    AND (@pTableName IS NULL OR t.name = @pTableName)
    AND (@pColumnName IS NULL OR c.name = @pColumnName)
ORDER BY    
    sc.name, t.name, c.name
OPTION(RECOMPILE);

open colcur
fetch next from colcur into @SchemaName, @TableName, @ColumnName

while @@fetch_status = 0 
begin

    if @Debug = 1
       print '--' + quotename(@SchemaName) + '.' + quotename(@TableName) + '.' + quotename(@ColumnName)

    select 
        @TableRowCount = sum(rows)
    from 
        sys.partitions p 
        join sys.tables t on p.object_id = t.object_id
        join sys.schemas sc on sc.schema_id = t.schema_id
    where 
        sc.name = @SchemaName
        and t.name = @TableName;

	/*
		Sample rate:
			if the parameter is -1, use the average of the sample rate of all stats on the table
			if the parameter is 0, use the max sample rate of all stats on the table
			otherwise use the sample rate that was supplied
	*/
	select
		@SamplePercent = 
		case 
			when @SamplePercent = -1 then isnull(nullif(cast(round(avg((100. * sp.rows_sampled) / sp.unfiltered_rows),0) as int),0),1)
			when @SamplePercent = 0 then isnull(nullif(cast(round(max((100. * sp.rows_sampled) / sp.unfiltered_rows),0) as int),0),1)				
			else @SamplePercent
		end
	from sys.tables t 
	join sys.stats s on t.object_id = s.object_id
	join sys.columns c on t.object_id = c.object_id	
	join sys.schemas sc on sc.schema_id = t.schema_id
	cross apply sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
	where 
        s.has_filter = 0
		and t.name = @TableName
		and sc.name = @SchemaName
		and c.name = @ColumnName
		and index_col(sc.name + '.' + t.name, s.stats_id, 1) = @ColumnName
		
	-- Generate the base distibution histogram: value, freq(value) 
	set @tallysql = 
		'select 
			[key] = ' + quotename(@ColumnName) + ', 
			x = dense_rank() over (order by ' + quotename(@ColumnName) + '), 
			f = count_big(1)			
		from ' + quotename(@SchemaName) + '.' + quotename(@TableName) + '
		tablesample	(' + cast(@SamplePercent as nvarchar(3)) + ' percent)
    	where ' + quotename(@ColumnName) + ' is not null 
		group by ' + quotename(@ColumnName) + ';'

	set @sql = 'insert into #tmp_tally_table ' + @tallysql;

    if @Debug = 1
		print @sql;
    else        
		exec sp_executesql @sql;

	/*
		calculate skewness stats: 
			b1 -> population skewness calculation
			G1 -> sample skewness calculation (corrects for sample bias)
			ses -> standard error of skew. This is a standard deviation calculation on the G1
			Zg1 -> test statistic which provides more meaning to the skewness calculation
	*/

	set @sql = 
	'insert #table_analysis
     ( 
        schema_name,
		table_name,
		column_name,
		b1,
		G1,
		ses,
		distinct_values,
		total_values,
		distinct_ratio,
		table_rows,
		distinct_ratio_null,
		Zg1,
        tallysql
    )
	select 
		''' + @SchemaName + ''',
		''' + @TableName + ''',
		''' + @ColumnName + ''',
		sk.b1,
		sk.G1,
		sk.ses,
		d,
		n,
		(100. * d)/n,
		' + cast(@TableRowCount as nvarchar(255)) + ',
		(100.*d)/' + cast(@TableRowCount as nvarchar(255)) + ',
		zG1 = case sk.ses when 0 then sk.G1 else sk.G1/sk.ses end,
        ''' + @tallysql + '''
	from (
			select 
				b1 =
					case 
                        when (power(m2,1.5)) = 0 then 0 
					    else m3 / (power(m2,1.5))
					end,
				G1 = 
                    case 
                        when n <= 2 then 0
                        else
                            (sqrt(1.*(n*(n-1)))/(n-2)) * 
					        case 
                                when (power(m2,1.5)) = 0 then 0 
					            else m3 / (power(m2,1.5))
					        end
                    end,
				ses = case 
                        when ((n-2.)*(n+1.)*(n+3.)) = 0 then 0
					    else sqrt(((1.*(6.*n)*(n-1.)))/((n-2.)*(n+1.)*(n+3.)))
					  end,
				d,n
			from (
					select 
						n,
						d,
						m2 = sum(power((x-(sxf/n)),2)*f)/n,
						m3 = sum(power((x-(sxf/n)),3)*f)/n
					from (
							select 
								x,
								f,
								sxf = 1. * sum(x*f) over(),
								n = sum(f) over(),
								d = count(x) over()
							from #tmp_tally_table
						) base
					group by n,d
			) agg
	) sk';

    if @Debug = 1
		print @sql;
    else        
		exec sp_executesql @sql;

	set @sql = 'truncate table #tmp_tally_table;';
    if @Debug = 1
		print @sql;
    else
	    exec sp_executesql @sql;

    fetch next from colcur into @SchemaName, @TableName, @ColumnName
end
close colcur;
deallocate colcur;

drop table if exists #tmp_tally_table;

-- The critical value of Zg1 is approximately 2. (This is a two-tailed test of skewness ≠ 0 at roughly the 0.05 significance level)
--
-- If Zg1 < −2, the population is very likely skewed negatively (though you don’t know by how much).
-- If Zg1 is between −2 and +2, you can’t reach any conclusion about the skewness of the population: it might be symmetric, or it might be skewed in either direction.
-- If Zg1 > 2, the population is very likely skewed positively (though you don’t know by how much).

select 
    *
from 
    #table_analysis
where 
    abs(Zg1) > 2
	and distinct_ratio <= 50
order by 
    abs(Zg1) desc;

--drop table #table_analysis

END