package org.sunbird.core.util
import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}

object EmbeddedMongo {

  private val starter: MongodStarter = MongodStarter.getDefaultInstance
  private val port: Int = 27017
  private var mongodExecutable: MongodExecutable = _
  private var mongodProcess: MongodProcess = _

  def start(): Unit = {
    mongodExecutable = starter.prepare(new MongodConfigBuilder()
      .version(Version.Main.V4_0)
      .net(new Net(port, false))
      .build())
    mongodProcess = mongodExecutable.start()
  }

  def close(): Unit = {
    mongodExecutable.stop()
  }

}
