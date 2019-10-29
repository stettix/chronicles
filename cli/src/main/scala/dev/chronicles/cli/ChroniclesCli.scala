package dev.chronicles.cli

import java.nio.file.{Path, Paths}

import cats.data.ValidatedNel
import cats.effect.{Clock, ExitCode, IO, IOApp}
import cats.implicits._
import com.monovore.decline._
import dev.chronicles.core.VersionTracker.{UpdateMessage, UserId}
import dev.chronicles.core.{InMemoryVersionTracker, TableName, VersionTracker, VersionedMetastore}
import dev.chronicles.db.DbVersionTracker
import doobie.util.transactor.Transactor
import pureconfig.{ConfigObjectSource, ConfigSource}

/**
  * A command line application that interacts with version data and metastores
  * in order to query and update the status of these.
  */
object ChroniclesCli extends IOApp {

  private val configPath = ".chronicles/config"

  override def run(args: List[String]): IO[ExitCode] =
    for {
      console <- Console[IO]
      homeDir <- getUserHomeDirectory
      config <- loadConfig(ConfigSource.file(homeDir.resolve(configPath)), console)
      client <- createClient(config, console)
      userId <- getUserName
      result <- run(args, client, console, userId)
    } yield result

  private[cli] def run(
      args: List[String],
      client: CliClient[IO],
      console: Console[IO],
      userId: UserId): IO[ExitCode] = {

    def parseArguments(console: Console[IO]): IO[Action] = argParser.parse(args) match {
      case Right(action) => IO(action)
      case Left(help) =>
        console.errorln(help.toString) >>
          IO.raiseError(new Error("Invalid Arguments"))
    }

    for {
      action <- parseArguments(console)
      _ <- client.executeAction(action, userId)
    } yield ExitCode.Success
  }

  private[cli] def loadConfig(configSource: ConfigObjectSource, console: Console[IO]): IO[Config] = {
    // Things compile without this first import, but results in failures at runtime!
    import pureconfig.module.enumeratum._
    import pureconfig.generic.auto._

    configSource.load[Config] match {
      case Left(errors) =>
        val errorDetails = errors.toList.map(_.description).mkString("\n")
        console.errorln(s"Failed to read configuration:\n$errorDetails") >>
          IO.raiseError(new Error("Invalid configuration"))
      case Right(config) =>
        IO.pure(config)
    }
  }

  private def createClient(config: Config, console: Console[IO]): IO[CliClient[IO]] = {
    for {
      versionTracker <- repositoryFromConfig(config)
      metastore = new StubMetastore[IO]
      delegate = new VersionedMetastore(versionTracker, metastore)
    } yield new CliClient[IO](delegate, console, Clock[IO])
  }

  private def repositoryFromConfig(config: Config): IO[VersionTracker[IO]] = config match {
    case MemConfig => InMemoryVersionTracker[IO]
    case d: DbConfig =>
      val transactor =
        Transactor.fromDriverManager[IO](d.parameters.driverClassName, d.parameters.jdbcUrl, d.username, d.password)
      IO.pure(DbVersionTracker(transactor))
  }

  private val getUserName: IO[UserId] =
    IO.fromEither(
      Option(System.getProperty("user.name"))
        .map(UserId)
        .toRight(new Error(s"Failed to get username")))

  private val getUserHomeDirectory: IO[Path] =
    IO.fromEither(
      Option(System.getProperty("user.home"))
        .map(Paths.get(_))
        .toRight(new Error("Failed to get user home directory"))
    )
  private[cli] val argParser: Command[Action] = {
    def validatedTableName(tableName: String): ValidatedNel[String, TableName] =
      TableName
        .fromFullyQualifiedName(tableName)
        .toOption
        .toValidNel(s"Invalid table name: '$tableName'. Should be in format <schema>.<table name>")

    val messageOption = Opts
      .option[String]("message", "Commit message for the operation")
      .map(UpdateMessage)

    val listTablesCommand = Opts.subcommand("tables", "List details about tables") {
      Opts.unit
        .map(_ => Action.ListTables)
    }

    val initTableCommand = Opts.subcommand("init", "Initialise version tracking for table") {
      (Opts
         .argument[String]("table name")
         .mapValidated(validatedTableName),
       Opts
         .flag("isSnapshot", "Indicates whether the new table is a snapshot table (i.e. a non-partitioned table)")
         .orFalse,
       messageOption).mapN(Action.InitTable)
    }

    val tableHistoryCommand = Opts.subcommand("log", "List version history for table") {
      Opts
        .argument[String]("table name")
        .mapValidated(validatedTableName)
        .map(Action.ShowTableHistory)
    }

    val listPartitionsCommand: Opts[Action] = Opts.subcommand("partitions", "List partitions for table") {
      Opts
        .argument[String]("table name")
        .mapValidated(validatedTableName)
        .map(Action.ListPartitions)
    }

    val modifyPartitionCommand: Opts[Action] =
      Opts.subcommand("partition", "Modify table partition") {
        (
          Opts
            .argument[String]("partition action")
            .mapValidated(str => PartitionOperation.parse(str).toValidNel("Invalid partition operation")),
          Opts.argument[String]("table name").mapValidated(validatedTableName),
          Opts.argument[String]("partition name"),
          messageOption
        ).mapN((partitionOperation, tableName, partitionName, message) =>
          partitionOperation match {
            case PartitionOperation.Add    => Action.AddPartition(tableName, partitionName, message)
            case PartitionOperation.Remove => Action.RemovePartition(tableName, partitionName, message)
        })
      }

    Command("chronicles", "Version control for tables") {
      listTablesCommand orElse
        initTableCommand orElse
        listPartitionsCommand orElse
        tableHistoryCommand orElse
        modifyPartitionCommand
    }
  }

}
