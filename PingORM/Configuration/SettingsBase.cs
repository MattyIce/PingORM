using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace PingORM.Configuration
{
    /// <summary>
    /// Base class for all configuration settings objects.
    /// </summary>
    public abstract class SettingsBase<T> : ConfigurationSection where T : ConfigurationSection
    {
        static log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// The specific settings for this game.
        /// </summary>
        public static T Current
        {
            get
            {
                if (_current == null)
                {
                    try
                    {
                        if (String.IsNullOrEmpty(GetSettingsFileLocation()))
                            return null;

                        ExeConfigurationFileMap fileMap = new ExeConfigurationFileMap() { ExeConfigFilename = GetSettingsFileLocation() };
                        System.Configuration.Configuration config = ConfigurationManager.OpenMappedExeConfiguration(fileMap, ConfigurationUserLevel.None);
                        _current = config.GetSection(GetSettingsSectionName()) as T;
                    }
                    catch (Exception ex) { Log.Error(String.Format("Error loading settings of type [{0}].", typeof(T).Name), ex); }
                }

                return _current;
            }
        }
        private static T _current;

        /// <summary>
        /// The XML version of the game settings.
        /// </summary>
        public static string Xml
        {
            get
            {
                if (_xml == null)
                    _xml = Current.SectionInformation.GetRawXml();

                return _xml;
            }
        }
        private static string _xml;

        /// <summary>
        /// Clears the cached settings so they will be reloaded from the game settings configuration file.
        /// </summary>
        public static void Refresh()
        {
            _current = null;
            _xml = null;
        }

        /// <summary>
        /// Gets the location on disk of the settings file for the current type of settings.
        /// </summary>
        /// <returns></returns>
        public static string GetSettingsFileLocation()
        {
            return ConfigurationManager.AppSettings[String.Format("{0}File", typeof(T).Name)];
        }

        /// <summary>
        /// Gets the configuration section name for the current type of settings.
        /// </summary>
        /// <returns></returns>
        public static string GetSettingsSectionName()
        {
            string name = typeof(T).Name;
            return String.Format("{0}{1}", name[0].ToString().ToLower(), name.Substring(1));
        }
    }
}
