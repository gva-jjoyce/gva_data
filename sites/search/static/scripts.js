const timestampFormat = "DD MMM YYYY HH:mm"

document.getElementById('start_date').valueAsDate = new Date();
document.getElementById('end_date').valueAsDate = new Date();

function htmlEncode(str) {
    if (str === undefined || str == null) { return '' }
    return str.toString().replace(/[\u00A0-\u9999<>\&]/gim, function(i) {
        return '&#' + i.charCodeAt(0) + ';'
    });
}

function is_date(sDate) {
    if (sDate === undefined || sDate == null) { return false }
    if (sDate.toString() == parseInt(sDate).toString()) return false;
    var tryDate = new Date(sDate);
    var m = moment(sDate, ['YYYY-MM-DD', 'YYYYMMDD', 'DD/MM/YYYY', moment.ISO_8601, 'L', 'LL', 'LLL', 'LLLL'], true);
    return (tryDate && tryDate.toString() != "NaN" && tryDate != "Invalid Date" && m.isValid());
}

function renderTable(data, page) {
    var page_size = 100
    var row_data = ''
    row_data += "<tr>"
    for (var h = 0; h < data.columns.length; h++) {
        row_data += "<th>" + htmlEncode(data.columns[h]) + "<th>"
    }
    row_data += "</tr>"
    var max_rows = page_size;
    if (data.length < max_rows) { max_rows = data.length; }
    for (var i = 0; i < max_rows; i++) {
        row_data += "<tr>";
        let index = i + (page * page_size);
        for (var h = 0; h < data.columns.length; h++) {
            var cell_value = data[index][data.columns[h]];
            console.log('TODO: if the field has already been identified as a date field')
            if (is_date(cell_value)) {
                row_data += "<td>" + moment(data[index][data.columns[h]]).format(timestampFormat) + "<td>"
            } else {
                row_data += "<td>" + htmlEncode(data[index][data.columns[h]]) + "<td>"
            }
        }
        row_data += "</tr>";
    }

    return "<table class='table table-striped table-sm'>" + row_data + "</table>";
}