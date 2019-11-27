#Requires -Version 3.0

param(
	[Parameter(Mandatory, Position = 0, HelpMessage = 'Enter URL to database to import file to')]
	[Uri]$URL,

	[Parameter(Mandatory, HelpMessage = 'Enter user email or token')]
	[Alias('u')]
	[string]$User,

	[Parameter(Mandatory, HelpMessage = 'Enter password')]
	[Alias('p')]
    [AllowEmptyString()]
	[string]$Password,

	[Parameter(Mandatory, HelpMessage = 'Enter path to CSV file to import')]
	[Alias('f')]
    [ValidateScript({Test-Path $_ -PathType Leaf})]
	[string]$File,

	[Parameter(Mandatory, HelpMessage = 'Enter table to import data to')]
	[Alias('a')]
	[string]$Table,

	[Parameter(Mandatory, HelpMessage = 'Enter column name to import data to or empty string to ignore')]
	[Alias('c')]
	[AllowEmptyString()]
	[string[]]$Columns = $null,

	[Parameter(HelpMessage = 'Run workflow rules')]
	[Alias('w')]
	[Switch]$Workflow = $false,

	[Parameter(HelpMessage = 'Enter name of unique column to match records with')]
	[ValidateNotNullOrEmpty()]
    [ValidateLength(1, 50)]
	[Alias('m')]
	[string]$Match,

	[Parameter()]
	[ValidateRange(0, 2147483647)]
	[int32]$Skip = 1,

	[Parameter()]
	[ValidateNotNullOrEmpty()]
	[Alias('l')]
	[string]$Culture = 'Default',

	[Parameter()]
    [ValidateLength(1, 1)]
	[Alias('d')]
	[string]$Delimiter,
	
	[Parameter()]
	[ValidateNotNullOrEmpty()]
	[Alias('e')]
	[string]$Encoding = 'Default',

    [Parameter()]
    [Alias('wi')]
    [switch]$WhatIf = $false
)
#
# Helper functions
#
# Call REST API
function Invoke-TeamDeskAPI {
	param ([string]$baseUrl, [string]$table, [string]$method, [string]$auth, [Parameter(ValueFromPipeline)][string]$body = "")
	
    try {
        $table = [Uri]::EscapeDataString($table.Replace("%", "%25").Replace("/", "%2F").Replace("\\", "%5C").Replace("?", "%3F"));
		$params = @{ Uri = "$baseUrl/$table/$method"; Headers = @{ "Authorization" = $auth } }
		if($body -ne "") { $params.Method = "Post"; $params.ContentType = "application/json;charset=utf-8"; $params.Body = $body }
		return Invoke-RestMethod @params
	} catch {
		$e = [string]$_
        # Try extract message from API
		try {
			$errObj = ConvertFrom-Json $e
			$errMsg = $errObj.message;
			if($errObj.source -ne $null) {
				$errMsg = $errObj.source + ": " + $errMsg
			}
            $errMsg = "REST API Error: $errMsg" 
		} catch {
			$errMsg = $e.Message
		}
		throw $errMsg
	}
}

# Convert date and numbers to locale independent variant
function Try-ConvertData {
	param($columnDefs, [string]$name, [string]$data, [Globalization.CultureInfo]$cultureObj)

	$cdef = ($columnDefs | where { $_.name -ieq $name })
	if($data -eq $null -or $data -eq "") {
		if($cdef.dataOptions -match "Required") {
			throw "Can not insert empty value into required column"
		}
		return $null;
	}
	$result = $null
	$type = $cdef.type;
	switch($type) {
		"Checkbox" {
			if($data -imatch "0|N|NO|FALSE") { return $false; }
			if($data -imatch "1|Y|YES|TRUE") { return $true; }
			break;
		}
		"Date" {
			[DateTimeOffset]$dtResult = [DateTimeOffset]::MinValue
			if([DateTimeOffset]::TryParse($data, $cultureObj, 0, [ref]$dtResult)) {
				return $dtResult.DateTime.Date.ToString("yyyy-MM-dd");
			}
			break;
		}
		"Time" {
			[DateTimeOffset]$dtResult = [DateTimeOffset]::MinValue
			if([DateTimeOffset]::TryParse($data, $cultureObj, 0, [ref]$dtResult)) {
				return $dtResult.DateTime.TimeOfDay.ToString("HH:mm:ss");
			}
			break;
		}
		"Timestamp" {
			[DateTimeOffset]$dtResult = [DateTimeOffset]::MinValue
			if([DateTimeOffset]::TryParse($data, $cultureObj, 0, [ref]$dtResult)) {
				return $dtResult.ToString("yyyy-MM-ddTHH:mm:sszzz");
			}
			break;
		}
		"Duration" {
			[TimeSpan]$tsResult = [TimeSpan]::MinValue
			if([TimeSpan]::TryParse($data, [ref]$tsResult)) {
				return [Math]::Floor($tsResult.Ticks / [TimeSpan]::TicksPerSecond);
			}
			break;
		}
		"Numeric" {
			[Decimal]$dcResult = [Decimal]::MinValue
			if([Decimal]::TryParse($data, [ref]$dcResult)) {
				return $dcResult;
			}
			break;
		}
        	"EMail" {
			try {
				$m = [MailAddress]$data
				return $m.Address
		    	} catch {
		    	}
		    	break;
        	}
		"User" {
			try {
				$m = [MailAddress]$data
				return $m.ToString()
		    	} catch {
		    	}
		    	break;
		}
		"URL" {
			[Uri]$uriResult
			if($data -imatch "https?://" -and [Uri]::TryCreate($data, [UriKind]::Absolute, [ref]$uriResult)) {
				return $data;
		   	}
		    	break;
		}
		default {
			if($cdef.width -and $data.Length -gt $cdef.width) {
				throw "Text exceeds maximum length of $($cdef.width) characters"
		    	}
			return $data
		}
	}
	throw "Can not convert '$value' to $type type"
}

#
# Validate and normalize arguments
#

# 1. Validate URL
if($URL -imatch "^(https://[^/]+/secure/(db|api/v2)/((?:\d+)(?:-\d+)?))(?:/.*|$)") {
    $URL = $matches[1] -ireplace "/db/", "/api/v2/"
} else {
    throw "-URL: parameter is not valid"
}

# 2. Create authorization header from either token or user/password
$apiAuth = "Bearer $user"
if(-not($User -imatch "^[0-9A-F]{32}$")) {
    $apiAuth = "Basic " + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${user}:${password}"))
}

# 3. Get Culture info
$cultureObj = if($Culture -eq "Default") { Get-Culture; } else { [Globalization.CultureInfo]::GetCultureInfo($culture) }
if($cultureObj.LCID -eq 4096) { throw "-Culture: parameter is not valid" }

# 4. Get defaults for delimiter
if($Delimiter -eq "") { $Delimiter = $cultureObj.TextInfo.ListSeparator; }

# 5. Prepare URL for Upsert call with options
$upsertURL = "upsert.json"
$extras = $null;
if($Match -ne "") { $extras += ,"match=" + [Uri]::EscapeDataString($Match) }
if(-not $Workflow) { $extras += ,"workflow=0" }
if($extras) { $upsertURL += "?" + ($extras -join "&") }

# 6. Name of the file to write errors to - file1.csv => file1.errors.csv
$errFile = [IO.Path]::ChangeExtension($File, ".errors$([IO.Path]::GetExtension($file))")
Remove-Item $errFile -ErrorAction SilentlyContinue -WhatIf:$WhatIf

#
# Get columns' information and check for validity
#
Write-Verbose "Retrieving database information"
$columnDefs = (Invoke-TeamDeskAPI $URL $Table "describe.json" $apiAuth -ErrorAction Stop).columns

# 7. Check if match column exists and unique
if($Match -ne "") {
	$cdef = $columnDefs | ? name -ieq $Match
	if($cdef -eq $null)  {
		throw "-Match: parameter is invalid. Column '$Match' not found"
	} elseif($cdef.dataOptions -notmatch "Unique") {
		throw "-Match: parameter is invalid. Column '$Match' is not unique"
	}
}

# 8. Check if all columns exist and updateable
$Columns | ? { $_ -ne $null -and $_ -ne "" -and $_ -ne "x" } | % {
    $column = $_
    $cdef = $columnDefs | ? name -ieq $column
	if($cdef -eq $null) {
		throw "-Columns: parameter is invalid. Column '$column' not found"
	} elseif($cdef.type -eq "Attachment" -or $cdef.readOnly -or $cdef.kind -ne "Updatable" -and $cdef.kind -ne "Key" -and $cdef.kind -ne "RecordOwner") {
		throw "-Columns: parameter is invalid. Column '$column' is not updateable"
	}
}

# 9. Import CSV does not allow empty headers, create "--ignore-N" fake headers instead
$csvheaders = $Columns | %{ $i = 1 }{ Write-Output $(if($_ -eq $null -or $_ -eq "" -or $_ -eq "x") { "--ignore-{0}" -f $i++ } else { $_ }) }


# unique prop/column name to store row error information
$errProp = "--error-message"
#
$importErrors = 0

# Here we go!
Write-Verbose "Opening data file $File"
Write-Verbose "Using delimiter '$Delimiter' and culture $($cultureObj.Name)"

# Start importing CSV file
Import-Csv -Path $File -Delimiter $Delimiter -Encoding $Encoding -Header $csvheaders | 
# Skip specified number of lines
Select -Skip $Skip |
# Split the data by 500 record batches
% {
    $buffer = New-Object Collections.ArrayList(500)
} {
    [void]$buffer.Add($_)
    if($buffer.Count -eq $buffer.Capacity) {
        Write-Output @{ Data = $buffer }
        $buffer.Clear()
    }
} {
    if($Buffer.Count) { Write-Output @{ Data = $buffer } }
} |
# Then process batches
% {
    # Before first batch
    $rowNum = $skip
    $batchStartRow = $skip
} { # Process batch
    $batchStartRow = $rowNum
    $batchRows = $_.Data
    $batchRows | 
    % { # Before first row
	    Write-Verbose "Importing rows $($rowNum + 1) to $($rowNum + $batchRows.Count)"
        $apiBatchData = $null
        $apiBatchIndexes = $null # indexes in a batchRows array passed to api
        $batchIndex = -1;
	    $batchErrors = 0
    } { # Process row
        $batchRow = $_
        $rowNum++
        $batchIndex++
		$apiRowData = @{};
        # Add storage for error if any
        $batchRow | Add-Member -MemberType NoteProperty -Name $errProp -Value $null
        # Try convert CSV's text data into native types
        $batchRow.PSObject.Properties | ? Name -ne $errProp | % {
            $name = $_.Name
			$value = $_.Value
            if($name -notmatch "^--ignore-\d+$") {
                try {
					$value = Try-ConvertData $columnDefs $name $value $cultureObj
					$apiRowData.Add($name, $value)
				} catch {
					$errMsg = $name + ": " + [string]$_
					$batchRow.$errProp += ,$errMsg
                    $batchErrors++
					Write-Warning "Row ${rowNum}: $errMsg"
				}
            } else {
                # suppress value from ignored members
                $batchRow.$name = ""
            }
        }
        if(-not $batchRow.$errProp) {             
            $apiBatchData += ,$apiRowData; 
            $apiBatchIndexes += ,$batchIndex
        }
    } { # After last row in a batch
        if($WhatIf) {
            Write-Host "What if: performing the operation `"Upsert`" on target `"TeamDesk`""
        } elseif($apiBatchData.Length) {  # Is there something to send?
		    $apiBatchData | 
            % { $_ | Select -ExcludeProperty $errProp } |
		    ConvertTo-Json |
		    Invoke-TeamDeskAPI $URL $table $upsertURL $apiAuth -ErrorAction Stop | 
            % { $i = -1 } {
			    $i++
			    $r = $_
			    if($r.status -ge 400) {
				    $batchErrors++
				    $errMsg = $r.errors | % {
                        $e = $_.message.TrimEnd()
                        if($_.source) {
                            $e = $_.source + ": " + $e
                        }
                        $batchRowIndex = $apiBatchIndexes[$i]
                        $batchRows[$batchRowIndex].$errProp = ,$e
				        Write-Warning "Row $($batchStartRow + $batchRowIndex + 1): $e"
                    }
			    }
            }
        }
        if($batchErrors) {
            # Dump rows errors to error file
            $headersToSkip = if(-not $Skip -or $importErrors) { 1 } else { 0 }
            $importErrors++
		    $batchRows | ? $errProp -ne $null | % { $_.$errProp = "ERROR: " + ($_.$errProp -join ", "); Write-Output $_ } |
		    ConvertTo-Csv -Delimiter $delimiter -NoTypeInformation |
		    Select -Skip $headersToSkip |
		    Add-Content -Path $errFile -Encoding $encoding -WhatIf:$WhatIf
        }
    }
}
if($importErrors) {
	Write-Warning "Some rows were not imported."
    if(-not $WhatIf) {
        Write-Warning "They are written to '$errFile'"
    }
	exit 1
}
