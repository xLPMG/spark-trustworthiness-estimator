package me.lpmg.ste.data

object Filter {
  private val nonArticleKeywords = List(
    "Template:",
    "Wikipedia:",
    "Category:",
    "File:",
    "Image:",
    "Portal:",
    "Help:",
    "Draft:",
    "Module:",
    "MediaWiki:",
    "Special:",
    "Talk:",
    "User:",
    "Book:",
    "Education Program:",
    "TimedText:",
    "Media:",
    "Topic:",
    "Gadget:",
    "Gadget definition:",
    "Wikipedia talk:",
    "User talk:",
    "File talk:",
    "MediaWiki talk:",
    "Template talk:",
    "Help talk:",
    "Category talk:",
    "Portal talk:",
    "Draft talk:",
    "Module talk:",
    "TimedText talk:",
    "Gadget talk:",
    "Gadget definition talk:",
    "Education Program talk:",
    "Topic talk:",
    "Special talk:"
  )

  def isArticleTitle(title: String): Boolean = {
    if (title == null || title.isEmpty || title == "|") {
      return false
    } else if (!title.contains(":")) {
      // if the title does not contain a colon, it is an article title
      return true
    }

    for (key <- nonArticleKeywords) {
      if (title.startsWith(key)) {
        return false
      }
    }

    true
  }
}
