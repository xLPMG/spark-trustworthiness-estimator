package me.lpmg.ste.data

/**
  * This object contains the logic to further clean up the source data.
  */
object AdditionalSourceCleanup {

    def cleanupSource(source: String): String = {

        var cleanedSource = source

        // remove pipe operator
        if (cleanedSource.contains("|")) {
            cleanedSource = source.split("\\|")(0)
        }

        // removed parenthesis
        cleanedSource = cleanedSource.replaceAll("[\\[\\]{}()]", "")

        cleanedSource
    }
  
}
