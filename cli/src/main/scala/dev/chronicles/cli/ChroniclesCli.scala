package dev.chronicles.cli

import cats.data.ValidatedNel
import cats.effect.{Clock, ExitCode, IO, IOApp}
import com.monovore.decline._
import cats.implicits._
import dev.chronicles.core.VersionTracker.{UpdateMessage, UserId}
import dev.chronicles.core.{InMemoryVersionTracker, TableName, VersionedMetastore}

/**
  * A command line application that interacts with version data and metastores
  * in order to query and update the status of these.
  */
object ChroniclesCli extends IOApp {

  // TODO: Refactor this to be more testable by passing in console, IO to get user ID, and backend delegate.
  override def run(args: List[String]): IO[ExitCode] = {

    def parseArguments(console: Console[IO]): IO[Action] = argParser.parse(args) match {
      case Left(help)    => console.println(help.toString).flatMap(_ => IO.raiseError(new Error("Invalid Arguments")))
      case Right(action) => IO.pure(action)
    }

    val getUserName: IO[UserId] =
      IO.fromEither(Option(System.getProperty("user.name")).map(UserId).toRight(new Error(s"Failed to get username")))

    for {
      console <- Console[IO]
      userId <- getUserName
      action <- parseArguments(console)
      _ <- execute(action, userId, console)
    } yield ExitCode.Success
  }

  private def execute(action: Action, userId: UserId, console: Console[IO]): IO[Unit] =
    for {
      config <- loadConfig(console)
      client <- createClient(config)
      _ <- client.executeAction(action, userId)
    } yield ()

  private def loadConfig(console: Console[IO]): IO[Config] = {
    // TODO:
    //   Try to read config from file
    //   Try to parse the file content

    IO.pure(Config())
  }

  private def createClient(config: Config): IO[VersionRepositoryClient[IO]] = {
    for {
      console <- Console[IO]
      versionTracker <- InMemoryVersionTracker[IO]
      metastore = new StubMetastore[IO]
      delegate = VersionedMetastore(versionTracker, metastore)
    } yield new VersionRepositoryClient[IO](delegate, console, Clock[IO])
  }

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
