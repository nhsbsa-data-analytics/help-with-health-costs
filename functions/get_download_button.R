#function to create button to display in markdown file,
#assigning data used in chart to this button will download data as csv
#when button is clicked in html output file webpage
#example: get_download_button(title = "Download chart data", 
#                             data = figure_1_data, 
#                             filename = "figure_1")

get_download_button <-
  function(data = data,
           title = "Download chart data",
           filename = "data") {
    dt <- datatable(
      data,
      rownames = FALSE,
      extensions = 'Buttons',
      options = list(
        searching = FALSE,
        paging = TRUE,
        bInfo = FALSE,
        pageLength = 1,
        dom = '<"datatable-wrapper"B>',
        buttons = list(
          list(
            extend = 'csv',
            text = title,
            filename = filename,
            className = "nhs-button-style"
          )
        ),
        initComplete = JS(
          "function(settings, json) {",
          "$(this.api().table().node()).css('visibility', 'collapse');",
          "}"
        )
      )
    )
    
    return(dt)
  }