# name of SSAS server
$serverName="localhost\yourSSASservername"

# load the AMO into the current runspace
[System.Reflection.Assembly]::LoadwithpartialName("Microsoft.AnalysisServices")
[System.Reflection.Assembly]::LoadwithpartialName("Microsoft.AnalysisServices.Tabular")

# connect to the server
$svr = new-Object Microsoft.analysisservices.tabular.Server
$svr.Connect($serverName)
# get current time and date
$currentTime = Get-Date
# counter to be used in for loop
$i = 0
# initialize variables
$PartitionToBeChecked =""
$db = $null
$tab = $null
$partitionsList = $null

# example:
# DB Name on SSAS: ABCXYZ2020
# Table needs to be partitioned: TXN
# Field to be used for partitioning: TXN_TIME
# Partition interval: monthly
# First month of data: January 2020
# Initial source query: "let Source = Oracle.Database(`"YourOracleServerIP:1521/YourOracleServerName`", [HierarchicalNavigation=true, Query=`"select user_id, txn_id, txn_type, txn_time, status, amount#(lf)from YourOracleTable #(lf)where TXN_TIME >= '#thisMonth' AND TXN_TIME < '#nextMonth'`"]),

# INSERT IN "databases" ARRAY IN THE FORM OF:
#--------------------------------- index 0 ------------------------------#--------------------------------- index 1 ----------------------------#---- index n ----#
# ((DB Name, (table, field, interval, first month, initial source query)),(DB Name, (table, field, interval, first month, initial source query)),... )
# IN CASE OF PARTITIONING 2 OR MORE TABLES FROM A DATABASE:
#------------------------------------------------------------------ index 0 ---------------------------------------------------------#--------------------------------- index 1 ----------------------------#---- index n ----#
# ((DB Name, (table, field, interval, first month, initial source query),(table, field, interval, first month, initial source query)),(DB Name, (table, field, interval, first month, initial source query)),... )
#
# Initial source query needs to be changed in order for this dynamic partition to work.
# Each initial source query has to change the time condition to use #thisMonth and #nextMonth flag.
# For example: 
# WHERE TXN_TIME >= '01jan21' =========> WHERE TXN_TIME >= '#thisMonth' AND TXN_TIME < '#nextMonth'
# This step is crucial for automating the separation of data using chosen partition interval.
# NOTE: There should be at least 2 elements inside of the "databases" array since the script would not run with only one element. You can duplicate the element inside the array if only partitioning one database
# The script works for yearly, monthly, and daily parition intervals.
$databases = @( 
    ("ABCXYZ2020", 
        ("TXN","TXN_TIME","month","Jan20",
                "let Source = Oracle.Database(`"YourOracleServerIP:1521/YourOracleServerName`", [HierarchicalNavigation=true, Query=`"select user_id, txn_id, txn_type, txn_time, status, amount#(lf)from YourOracleTable #(lf)where TXN_TIME >= '#thisMonth' AND TXN_TIME < '#nextMonth'`"])")),

    ("ABCXYZ2020", 
        ("TXN","TXN_TIME","month","Jan20",
                "let Source = Oracle.Database(`"YourOracleServerIP:1521/YourOracleServerName`", [HierarchicalNavigation=true, Query=`"select user_id, txn_id, txn_type, txn_time, status, amount#(lf)from YourOracleTable #(lf)where TXN_TIME >= '#thisMonth' AND TXN_TIME < '#nextMonth'`"])")),
        )


function Partition_Database ($database) {
    $j = 1
    # get how many items in the index, or how many tables need to be partitioned
    $itemLength = @($db).Count
    # get the database reference from the SSAS server
    $db = $svr.Databases.GetByName($database[0])

    # partition by year
    if ($database[$j][2] -eq "year") 
    {
        # loop traversing through all the items in the index
        while ( $j -le ($itemLength-1)) 
        {
            $i = 0
            # find the table that needs to be partitioned
            $tab = $db.Model.Tables.Find($database[$j][0])
            # get the list of partitions
            $partitionsList = $tab.Partitions.Name
            # create the partition name
            $partitionName = $database[$j][0] + "_" + $database[$j][3].Substring(3)

            while($PartitionToBeChecked  -ne $database[$j][0] + "_" + $database[$j][3].Substring(3)) 
            {
                $thisMonth= "01Jan" + $currentTime.AddYears($i).ToString("yy")
                $nextMonth= "01Jan" + $currentTime.AddYears($i+1).ToString("yy")

                $PartitionToBeChecked = $database[$j][0] + "_" + $currentTime.AddYears($i).ToString("yy")
                $partitionsList = $tab.Partitions.Name

                # if the partition is not created yet, then create one and then process it
                if ($partitionsList -contains $PartitionToBeChecked -eq $False)
                {
                    # creating new partition
                    $newPartition = new-object Microsoft.AnalysisServices.Tabular.Partition
                    $newpartition.Source = New-Object -TypeName Microsoft.AnalysisServices.Tabular.MPartitionSource;
                    $newpartition.Mode = [Microsoft.AnalysisServices.Tabular.ModeType]::Default;
                    $newPartition.Name = $PartitionToBeChecked
                    # replacing #thisMonth and #nextMonth flags to get dynamic time queries
                    $templateSource = $database[$j][4] -replace "#thisMonth", "$thisMonth"
                    $newPartition.Source.Expression = $templateSource -replace "#nextMonth", "$nextMonth"
                    # adding the created partition in the list of partitions
                    $tab.Partitions.Add($newPartition)
                    Write-Output("Updating Databases")
                    # do a database update to import the new partition to the database
                    $db.Update([Microsoft.AnalysisServices.UpdateOptions]::ExpandFull)
                    Write-Output("Partition " + $PartitionToBeChecked + " has been created successfully for table " + $tab.Name + " in database " + $database[0])
                    # process the new partition
                    Invoke-ProcessPartition -PartitionName $PartitionToBeChecked -TableName $database[$j][0] -Database $database[0] -RefreshType "Automatic" -Server $serverName
                    Write-Output("Partition " + $PartitionToBeChecked + " has been processed successfully for table " + $tab.Name + " in database " + $database[0])
                }
                # if the partition is created, process it
                else
                {
                    Invoke-ProcessPartition -PartitionName $PartitionToBeChecked -TableName $database[$j][0] -Database $database[0] -RefreshType "Automatic" -Server $serverName
                    Write-Output("Partition " + $PartitionToBeChecked + " has been processed successfully for table " + $tab.Name + " in database " + $database[0])
                }
                $i--
            }
            $j++
        }
    } 

    #partition by month
    elseif ($database[$j][2] -eq "month")
    {
        # loop traversing through all the items in the index
        while ( $j -le ($itemLength-1)) 
        {
            $i = 0
            # find the table that needs to be partitioned
            $tab = $db.Model.Tables.Find($database[$j][0])
            # get the list of partitions
            $partitionsList = $tab.Partitions.Name
            # create the partition name
            $partitionName = $database[$j][0] + "_" + $database[$j][3]

            while($PartitionToBeChecked  -ne $partitionName)

            {
                $thisMonth= "01" + (Get-Culture).DateTimeFormat.GetAbbreviatedMonthName($currentTime.AddMonths($i).Month) + $currentTime.AddMonths($i).ToString("yy")
                $nextMonth= "01" + (Get-Culture).DateTimeFormat.GetAbbreviatedMonthName($currentTime.AddMonths($i+1).Month) + $currentTime.AddMonths($i+1).ToString("yy")

                $PartitionToBeChecked=$database[$j][0] + "_" + (Get-Culture).DateTimeFormat.GetAbbreviatedMonthName($currentTime.AddMonths($i).Month) + $currentTime.AddMonths($i).ToString("yy")

                # if the partition is not created yet, then create one and then process it
                if ($partitionsList -contains $PartitionToBeChecked -eq $False)
                {
                    # creating new partition
                    $newPartition = new-object Microsoft.AnalysisServices.Tabular.Partition
                    $newpartition.Source = New-Object -TypeName Microsoft.AnalysisServices.Tabular.MPartitionSource;
                    $newpartition.Mode = [Microsoft.AnalysisServices.Tabular.ModeType]::Default;
                    $newPartition.Name = $PartitionToBeChecked
                    # replacing #thisMonth and #nextMonth flags to get dynamic time queries
                    $templateSource = $database[$j][4] -replace "#thisMonth", "$thisMonth"
                    $newPartition.Source.Expression =  $templateSource -replace "#nextMonth", "$nextMonth"
                    # adding the created partition in the list of partitions
                    $tab.Partitions.Add($newPartition)
                    Write-Output("Updating Databases")
                    # do a database update to import the new partition to the database
                    $db.Update([Microsoft.AnalysisServices.UpdateOptions]::ExpandFull)
                    Write-Output("Partition " + $PartitionToBeChecked + " has been created successfully for table " + $tab.Name + " in database " + $database[0])
                    # process the new partition
                    Invoke-ProcessPartition -PartitionName $PartitionToBeChecked -TableName $database[$j][0] -Database $database[0] -RefreshType "Automatic" -Server $serverName
                    Write-Output("Partition " + $PartitionToBeChecked + " has been processed successfully for table " + $tab.Name + " in database " + $database[0])
                }
                # if the partition is created, process it
                else
                {
                    Invoke-ProcessPartition -PartitionName $PartitionToBeChecked -TableName $database[$j][0] -Database $database[0] -RefreshType "Automatic" -Server $serverName
                    Write-Output("Partition " + $PartitionToBeChecked + " has been processed successfully for table " + $tab.Name + " in database " + $database[0])
                }
                $i--

            }
            $j++
        }
    }

    #partition by day
    elseif ($database[$j][2] -eq "day")
    {
        # loop traversing through all the items in the index
        while ( $j -le ($itemLength-1)) 
        {
            $i = 0
            # find the table that needs to be partitioned
            $tab = $db.Model.Tables.Find($database[$j][0])
            # get the list of partitions
            $partitionsList = $tab.Partitions.Name
            # create the partition name
            $partitionName = $database[$j][0] + "_" + $database[$j][3]

            while($PartitionToBeChecked  -ne $partitionName)

            {
                $thisMonth= $currentTime.AddDays($i).Day.ToString("00") + (Get-Culture).DateTimeFormat.GetAbbreviatedMonthName($currentTime.AddDays($i).Month) + $currentTime.AddDays($i).ToString("yy")
                $nextMonth= $currentTime.AddDays($i+1).Day.ToString("00") + (Get-Culture).DateTimeFormat.GetAbbreviatedMonthName($currentTime.AddDays($i+1).Month) + $currentTime.AddDays($i+1).ToString("yy")

                $PartitionToBeChecked=$database[$j][0] + "_" + $currentTime.AddDays($i).Day.ToString("00") + (Get-Culture).DateTimeFormat.GetAbbreviatedMonthName($currentTime.AddDays($i).Month) + $currentTime.AddDays($i).ToString("yy")

                # if the partition is not created yet, then create one and then process it
                if ($partitionsList -contains $PartitionToBeChecked -eq $False)
                {
                    # creating new partition
                    $newPartition = new-object Microsoft.AnalysisServices.Tabular.Partition
                    $newpartition.Source = New-Object -TypeName Microsoft.AnalysisServices.Tabular.MPartitionSource;
                    $newpartition.Mode = [Microsoft.AnalysisServices.Tabular.ModeType]::Default;
                    $newPartition.Name = $PartitionToBeChecked
                    # replacing #thisMonth and #nextMonth flags to get dynamic time queries
                    $templateSource = $database[$j][4] -replace "#thisMonth", "$thisMonth"
                    $newPartition.Source.Expression =  $templateSource -replace "#nextMonth", "$nextMonth"
                    # adding the created partition in the list of partitions
                    $tab.Partitions.Add($newPartition)
                    Write-Output("Updating Database")
                    # do a database update to import the new partition to the database
                    $db.Update([Microsoft.AnalysisServices.UpdateOptions]::ExpandFull)
                    Write-Output("Partition " + $PartitionToBeChecked + " has been created successfully for table " + $tab.Name + " in database " + $database[0])
                    # process the new partition
                    Invoke-ProcessPartition -PartitionName $PartitionToBeChecked -TableName $database[$j][0] -Database $database[0] -RefreshType "Automatic" -Server $serverName
                    Write-Output("Partition " + $PartitionToBeChecked + " has been processed successfully for table " + $tab.Name + " in database " + $database[0])
                }
                # if the partition is created, process it
                else
                {
                    Write-Output("Processing " + $PartitionToBeChecked + " for table " + $database[$j][0])
                    Invoke-ProcessPartition -PartitionName $PartitionToBeChecked -TableName $database[$j][0] -Database $database[0] -RefreshType "Automatic" -Server $serverName
                    Write-Output("Partition " + $PartitionToBeChecked + " has been processed successfully for table " + $tab.Name + " in database " + $database[0])
                }
                $i--

            }
            $j++
        }
    }
    ## save the changes back to the database on the server
    #$db.Update([Microsoft.AnalysisServices.UpdateOptions]::ExpandFull)
}

# for loop to check database name and find its info
foreach ($db in $databases) 
{
    Partition_Database $db
}

$svr.Disconnect()
