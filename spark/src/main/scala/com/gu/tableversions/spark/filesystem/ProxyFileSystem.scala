package com.gu.tableversions.spark.filesystem

import java.net.URI

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

/**
  * A ProxyFileSystem wraps the behaviour of some other ("underlying" or "base") FileSystem,
  * enabling transformations of `Path`s provided to the base `FileSystem` through the use of the
  * `PathMapper`. Operations on the `ProxyFileSystem` ultimately delegate to the base filesystem,
  * but first translate the paths provided to the `ProxyFileSystem` into a format suitable for
  * that base filesystem.
  *
  * `Path` values returned by calls to the base filesystem are translated in the opposite
  * direction by the `PathMapper`, making them suitable for use in future calls to the ProxyFileSystem.
  */
abstract class ProxyFileSystem extends FileSystem {

  import ProxyFileSystem._

  // The underlying FileSystem we delegate to. Paths in calls to this FileSystem
  // should be translated with the PathMapper.
  @volatile protected var baseFs: FileSystem = _

  // The root URI for the underlying FileSystem
  @volatile protected var baseUri: URI = _

  // A utility to map between Paths for the proxy and underlying FileSystems.
  @volatile protected var pathMapper: PathMapper = _

  /** Call from concrete implementation to initialise the base class. */
  protected def initialiseProxyFileSystem(_baseUri: URI, _baseFilesystem: FileSystem, _pathMapper: PathMapper): Unit = {
    baseUri = _baseUri
    pathMapper = _pathMapper
    baseFs = _baseFilesystem
  }

  override def getUri: URI =
    new URI(s"${VersionedFileSystem.scheme}:${baseUri.getSchemeSpecificPart}")

  override def open(f: Path, bufferSize: Int): FSDataInputStream =
    baseFs.open(pathMapper.forUnderlying(f), bufferSize)

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    baseFs.create(pathMapper.forUnderlying(f), permission, overwrite, bufferSize, replication, blockSize, progress)

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    baseFs.append(pathMapper.forUnderlying(f), bufferSize, progress)

  override def rename(src: Path, dst: Path): Boolean =
    baseFs.rename(pathMapper.forUnderlying(src), pathMapper.forUnderlying(dst))

  override def delete(f: Path, recursive: Boolean): Boolean =
    baseFs.delete(pathMapper.forUnderlying(f), recursive)

  override def listStatus(f: Path): Array[FileStatus] =
    baseFs.listStatus(pathMapper.forUnderlying(f)).map(status => mapFileStatusPath(status, pathMapper.fromUnderlying))

  override def setWorkingDirectory(new_dir: Path): Unit =
    baseFs.setWorkingDirectory(pathMapper.forUnderlying(new_dir))

  override def getWorkingDirectory: Path =
    pathMapper.fromUnderlying(baseFs.getWorkingDirectory)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    baseFs.mkdirs(pathMapper.forUnderlying(f), permission)

  override def getFileStatus(f: Path): FileStatus =
    mapFileStatusPath(baseFs.getFileStatus(pathMapper.forUnderlying(f)), pathMapper.fromUnderlying)

  override def setReplication(src: Path, replication: Short): Boolean =
    baseFs.setReplication(pathMapper.forUnderlying(src), replication)

  override def setTimes(p: Path, mtime: Long, atime: Long): Unit =
    baseFs.setTimes(pathMapper.forUnderlying(p), mtime, atime)

  override def setOwner(f: Path, username: String, groupname: String): Unit =
    baseFs.setOwner(pathMapper.forUnderlying(f), username, groupname)

  override def getFileChecksum(f: Path): FileChecksum =
    baseFs.getFileChecksum(pathMapper.forUnderlying(f))

  // Apply a function to the path in the provided FileStatus
  private def mapFileStatusPath(fileStatus: FileStatus, f: Path => Path): FileStatus =
    new FileStatus(
      fileStatus.getLen,
      fileStatus.isDirectory,
      fileStatus.getReplication.toInt,
      fileStatus.getBlockSize,
      fileStatus.getModificationTime,
      f(fileStatus.getPath)
    )
}

object ProxyFileSystem {

  /**
    * Map paths between one "outer" FileSystem (e.g. a ProxyFileSystem) and an underlying one.
    * For example, by changing the schema, or appending version directories.
    */
  trait PathMapper {

    /** Convert a path with a "versioned" scheme to one suitable for the underlying FileSystem. */
    def forUnderlying(path: Path): Path

    /** Convert a path from the underlying FileSystem to one in the "versioned" scheme. */
    def fromUnderlying(path: Path): Path
  }

}
