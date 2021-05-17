url <- "https://noi.md/md/societate/coronavirus-in-moldova-statistica-infectarilor-pe-teritoriul-tarii-428097"

count <- read_html(url) %>%
    html_nodes('p') %>%
    magrittr::extract(7) %>%
    html_text() %>%
    str_replace_all(fixed(" "), "") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Moldova",
    country = "Moldova",
    units = "tests performed",
    source_url = url,
    source_label = "Moldova Ministry of Health, Labour and Social Protection",
    testing_type = "includes non-PCR"
)
