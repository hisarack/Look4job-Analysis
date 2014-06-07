import AssemblyKeys._

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
      case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
      case PathList("com", "esotericsoftware", "minlog", xs @ _*) => MergeStrategy.first
      case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
      case "application.conf" => MergeStrategy.concat
      case "unwanted.txt"     => MergeStrategy.discard
      case x => old(x)
}}
