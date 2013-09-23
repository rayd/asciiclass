# worldcup.awk

function print_years(country, place, str) {
    beginning=match(str, "\(")+1
    len=match(str, "\)") - beginning
    years_str = substr(str, beginning, len)
    split(years_str, years_array, ", ")
    for(i in years_array)
        print country","years_array[i]","place
}

BEGIN { RS="\|\-\n"; FS="\n"; print "Country,Year,Place"}
/^[A-Z]+/ {
    country=$1
    print_years(country, 1, $2)
    print_years(country, 2, $3)
    print_years(country, 3, $4)
    print_years(country, 4, $5)
}
