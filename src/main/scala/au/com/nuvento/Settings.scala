package au.com.nuvento


import com.typesafe.config.{Config, ConfigFactory}

import java.util.Properties

class Settings extends Serializable {
	val defaultConfig = ConfigFactory.load()

	val postgres = defaultConfig.getConfig("config.postgres")
	val azure = defaultConfig.getConfig("config.azure")
	def setProperties(settings: Config): Properties = {
		val properties = new Properties()
		import scala.collection.JavaConverters._

		settings.entrySet().asScala.foreach (x => {
			if (x.getValue.unwrapped () == true) {
				properties.put (x.getKey, x.getValue.render () )
			}
		})
		properties
	}

}
