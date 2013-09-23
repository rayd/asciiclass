# synsets.awk

BEGIN { FS = ","}
/^[0-9]+/ {
    split($2, words, " ")
    split($3, definitions, "; ")
    for (i in words)
        for (j in definitions)
            print words[i]","definitions[j]
}
