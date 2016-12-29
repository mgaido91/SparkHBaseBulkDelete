import AssemblyKeys._ // put this at the top of the file

assemblySettings

mergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "jasper", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// your assembly settings here
test in assembly := {}
