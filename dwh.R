#============================================================================
#  function: dwh_connect
#============================================================================
#' connect to DWH
#'
#' connect to datawarehouse (DWH) using ODBC
#'
#' @param dsn DSN string
#' @param user user name
#' @param pwd password of user
#' @param pwd_crypt is password encryption used?
#' @param timeout_sec timeout in sec for connecting with DWH
#' @param ... Further arguments to be passed to DBI::dbConnect()
#' @return connection
#' @examples
#' \dontrun{
#' con <- dwh_connect(dsn = "DWH1", user = "u12345")
#' }
#' @export

dwh_connect <- function(dsn, user = NA, pwd = NA, pwd_crypt = FALSE, timeout_sec = 15, ...)  {

  if (is.na(user))  {
    # use single sign on
    channel <- DBI::dbConnect(odbc::odbc(), dsn, ...)

  } else {
    # use user & passwort
    channel <- DBI::dbConnect(odbc::odbc(), dsn,
                              user = user,
                              password = if (pwd_crypt == TRUE) decrypt(pwd) else pwd,
                              timeout = timeout_sec,
                              ...
    )
  } # if
  return(channel)
}

#============================================================================
#  function: dwh_disconnect
#============================================================================
#' disconnect from DWH
#'
#' disconnect from datawarehouse (DWH) using a ODBC connection
#'
#' @param connection channel (ODBC connection)
#' @param ... Further arguments to be passed to DBI::dbDisconnect()
#' @examples
#' \dontrun{
#' dwh_disconnect(con)
#' }
#' @export

dwh_disconnect <- function(connection, ...)  {
  DBI::dbDisconnect(connection, ...)
}

#============================================================================
#  function: dwh_read_table
#============================================================================
#' read a table from DWH
#'
#' read a table from DWH using a ODBC connection
#'
#' @param connection DWH connection
#' @param table table name (character string)
#' @param names_lower convert field names to lower (default = TRUE)
#' @param ... Further arguments to be passed to DBI::dbGetQuery()
#' @return dataframe containing table data
#' @examples
#' \dontrun{
#' dwh_read_table(con, "database.table_test")
#' }
#' @export

dwh_read_table <- function(connection, table, names_lower = TRUE, ...)  {

  # define sql
  sql <- paste0("select * from ", table)

  # read data from dwh
  data <- DBI::dbGetQuery(connection, sql, ...)

  # convert names to lower case
  if (names_lower) names(data) <- tolower(names(data))

  return(data)
}

#============================================================================
#  function: dwh_read_data
#============================================================================
#' read data from DWH
#'
#' read data from DWH using a ODBC connection
#'
#' @param connection DWH connection
#' @param sql sql (character string)
#' @param names_lower convert field names to lower (default = TRUE)
#' @param ... Further arguments to be passed to DBI::dbGetQuery()
#' @return dataframe containing table data
#' @examples
#' \dontrun{
#' dwh_read_data(con, "select * from database.table_test")
#' }
#' @export

dwh_read_data <- function(connection, sql, names_lower = TRUE, ...)  {

  # read data from dwh
  data <- DBI::dbGetQuery(connection, sql, ...)

  # convert names to lower case
  if (names_lower) names(data) <- tolower(names(data))

  return(data)
}

#============================================================================
#  function: dwh_fastload
#============================================================================
#' write data to a DWH table
#'
#' write data fast to a DWH table using a ODBC connection
#' Function uses packages DBI/odbc to write data faster than RODBC
#' Connects, writes data and disconnects
#'
#' @param data dataframe
#' @param dsn DSN string
#' @param table table name (character string)
#' @param overwrite Overwrite table if already exist
#' @param append Append data to table
#' @param timeout_sec timeout in sec connecting to DWH
#' @param ... Further arguments to be passed to DBI::dbConnect()
#' @return status
#' @examples
#' \dontrun{
#' dwh_fastload(data, "DWH", "database.table_test")
#' }
#' @export

dwh_fastload <- function(data, dsn, table, overwrite = FALSE, append = FALSE, timeout_sec = 15, ...)  {

  # check table (must be 'database.table')
  # split string at '.'
  table_split <- strsplit(table, split="[.]")
  database_name <- table_split[[1]][1]
  table_name <- table_split[[1]][2]

  # valid database_name and table_name?
  if ( is.na(database_name) | is.na(table_name) )   {
    stop("table must be in the format 'database.table'")
  }
  stopifnot (nchar(database_name) > 0, nchar(table_name) > 0)

  # connect
  con <- DBI::dbConnect(odbc::odbc(), dsn=dsn, database=database_name, timeout = timeout_sec, ...)

  # write data
  DBI::dbWriteTable(con, name=table_name, value=data, overwrite=overwrite, append=append)

  # disconnect
  DBI::dbDisconnect(con)

} # dwh_fastload

#============================================================================
#  function: dwh_batchload
#============================================================================
#' write data to a DWH table by spliting into buckets
#'
#' write data fast to a DWH table using a ODBC connection
#' Function uses packages DBI/odbc to write data faster than RODBC
#' Connects, writes data and disconnects
#'
#' @param data dataframe
#' @param dsn DSN string
#' @param table table name (character string)
#' @param batch_size max. rows to be added to table in one step
#' @param timeout_sec max. time in sec. to wait until connected
#' @param wait_between_sec sec. waiting between buckets
#' @return status
#' @examples
#' \dontrun{
#' dwh_batchload(data, "DWH", "database.table_test", batch_size = 1000)
#' }
#' @export

dwh_batchload <- function(data, dsn, table, batch_size = 100000, 
                          timeout_sec = 15, wait_between_sec = 1)  {

  # cut into buckets
  steps <- ceiling(nrow(data) / batch_size)
  buckets <- cut(seq(1, nrow(data)), steps)
  nr_buckets <- length(levels(buckets))
  
  # fastload first bucket
  data2 <- data[buckets == levels(buckets)[1], ]
  cat("fastloading part", 1, "of", nr_buckets, ":",nrow(data2), "rows\n")
  dwh_fastload(data2, dsn, table, timeout = timeout_sec)
  
  # all observations in one bucket?
  if (steps == 1) {return(TRUE)}
  
  # fastload rest of buckets
  for (i in 2:length(levels(buckets))) {
    
    # wait a bit so that DWH is ready again
    Sys.sleep(wait_between_sec)
    
    # fastload next bucket
    data2 <- data[buckets == levels(buckets)[i], ]
    cat("fastloading part", i, "of", nr_buckets, ":",nrow(data2), "rows\n")
    dwh_fastload(data2, dsn, table, 
                        append = TRUE,
                        timeout = timeout_sec)
    
  } #for
  
  # everything should be fastloaded
  cat("done (total", nrow(data), "rows)\n")

  return(TRUE)
  
} #dwh_batchload
