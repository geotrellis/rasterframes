moduleName := "rasterframes-experimental"

libraryDependencies ++= Seq(
  geotrellis("s3").value,
  spark("core").value % Provided,
  spark("mllib").value % Provided,
  spark("sql").value % Provided,
  "net.virtual-void" %%  "json-lenses" % "0.6.2"
)