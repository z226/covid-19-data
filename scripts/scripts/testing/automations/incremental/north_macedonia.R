url <- "https://koronavirus.gov.mk/"

page <- read_html(url)

df <- page %>% html_node("#table_1") %>% html_table()

count <- str_replace_all(df$`Вкупно тестови`, "[^\\d]", "") %>% as.integer()

add_snapshot(
    count = count,
    sheet_name = "North Macedonia",
    country = "North Macedonia",
    units = "tests performed",
    source_url = url,
    source_label = "Ministry of Health",
    testing_type = "unclear"
)
