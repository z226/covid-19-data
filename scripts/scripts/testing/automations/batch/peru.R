url <- "https://datos.ins.gob.pe/dataset/f832f652-b8b5-47d0-811a-16bd6b5cf724/resource/2caa018e-5df8-47f6-a3b4-8415898b2f47/download/pm15junio2021.zip"

process_file <- function(url) {
    filename <- str_extract(url, "[^/]+\\.zip$")
    local_path <- sprintf("tmp/%s", filename)
    if (!file.exists(local_path)) {
        download.file(url = url, destfile = local_path)
    }
    csv_filename <- unzip(local_path, list = TRUE)$Name[1]
    unzip(local_path, exdir = "tmp")
    df <- fread(sprintf("tmp/%s", csv_filename), showProgress = FALSE, select = c("FECHATOMAMUESTRA", "RESULTADO"))
    setnames(df, c("Date", "Result"))
    df[, Date := as.character(Date)]
    return(df)
}

data <- process_file(url)

data <- data[Date <= today() & Date >= "2020-01-01" & !is.na(Date)]

df <- data[, .(
    `Daily change in cumulative total` = .N,
    `Positive` = sum(Result == "POSITIVO")
), Date]

setorder(df, Date)
df[, `Positive rate` := round(frollsum(Positive, 7) / frollsum(`Daily change in cumulative total`, 7), 3)]
df[, Positive := NULL]

df[, Country := "Peru"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://datos.ins.gob.pe/dataset?q=%22pruebas+moleculares%22&organization=covid-19"]
df[, `Source label` := "National Institute of Health"]
df[, `Testing type` := "PCR only"]
df[, Notes := NA]

fwrite(df, "automated_sheets/Peru.csv")

stopifnot(max(df$Date) > today() - days(14))
