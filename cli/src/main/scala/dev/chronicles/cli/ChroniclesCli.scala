package dev.chronicles.cli

import cats.data.ValidatedNel
import cats.effect.{ExitCode, IO, IOApp}
import com.monovore.decline._
import cats.implicits._
import dev.chronicles.core.TableName

/**
  * A command line application that interacts with version data and metastores
  * in order to query and update the status of these.
  */
object ChroniclesCli extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    (argParser.parse(args) match {
      case Left(help)    => printHelp(help)
      case Right(action) => execute(action)
    }).map(_ => ExitCode.Success)

  private def execute(action: Action): IO[Unit] = {

    for {
      console <- Console[IO]
      config <- loadConfig(console) // TODO: may need config from args at some point
      client <- createClient(config)
      _ <- client.executeAction(action)
    } yield ()

    // TODO:
    //   - Load config [later: or use optional config from args]
    //   - Create an instance of the client back-end defined in config, using the config
    //   - Execute actions via this back-end
    //   Initially I'll only have a dummy back-end for the in-memory implementation of version tracker (which is very useless for this...)
    //     ...but I can use a real metastore backend (this needs to be switchable in config too)
    //   But I can then add a backend that accesses the persistent version tracker directly (requires access to backing DB)
    //   And when I add a REST service on top of the tracker, I can add a client for this one too.

    ??? // TODO!
  }

  private def printHelp(help: Help): IO[Unit] =
    IO(println(help.toString))

  private def loadConfig(console: Console[IO]): IO[Config] = {
    // Try to read config from file

    // Try to parse the file content

    ???
  }

  private def createClient(config: Config): IO[VersionRepositoryClient[IO]] = ???

  private[cli] val argParser: Command[Action] = {
    def validatedTableName(tableName: String): ValidatedNel[String, TableName] =
      TableName
        .fromFullyQualifiedName(tableName)
        .toOption
        .toValidNel(s"Invalid table name: '$tableName'. Should be in format <schema>.<table name>")

    val listTablesCommand: Opts[Action] = Opts.subcommand("tables", "List details about tables") {
      Opts.unit
        .map(_ => Action.ListTables)
    }

    val tableHistoryCommand: Opts[Action] = Opts.subcommand("log", "List version history for table") {
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
          Opts.argument[String]("partition name")
        ).mapN((partitionOperation, tableName, partitionName) =>
          partitionOperation match {
            case PartitionOperation.Add    => Action.AddPartition(tableName, partitionName)
            case PartitionOperation.Remove => Action.RemovePartition(tableName, partitionName)
        })
      }

    Command("chronicles", "Version control for tables") {
      listTablesCommand orElse
        listPartitionsCommand orElse
        tableHistoryCommand orElse
        modifyPartitionCommand
    }
  }

}
