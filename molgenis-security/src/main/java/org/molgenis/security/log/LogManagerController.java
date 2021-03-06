package org.molgenis.security.log;

import static org.molgenis.security.log.LogManagerController.URI;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.molgenis.framework.ui.MolgenisPluginController;
import org.molgenis.security.core.utils.SecurityUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;

@Controller
@RequestMapping(URI)
public class LogManagerController extends MolgenisPluginController
{
	private static final Logger LOG = LoggerFactory.getLogger(LogManagerController.class);

	public static final String ID = "logmanager";
	public static final String URI = MolgenisPluginController.PLUGIN_URI_PREFIX + ID;

	private static final List<Level> LOG_LEVELS;

	static
	{
		LOG_LEVELS = Arrays.asList(Level.ALL, Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.OFF);
	}

	public LogManagerController()
	{
		super(URI);
	}

	@RequestMapping(method = RequestMethod.GET)
	public String init(Model model)
	{
		ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
		if (!(iLoggerFactory instanceof LoggerContext))
		{
			throw new RuntimeException("Logger factory is not a Logback logger context");
		}
		LoggerContext loggerContext = (LoggerContext) iLoggerFactory;

		List<Logger> loggers = new ArrayList<Logger>();
		for (ch.qos.logback.classic.Logger logger : loggerContext.getLoggerList())
		{
			if (logger.getLevel() != null || logger.iteratorForAppenders().hasNext())
			{
				loggers.add(logger);
			}
		}

		model.addAttribute("loggers", loggers);
		model.addAttribute("levels", LOG_LEVELS);
		model.addAttribute("hasWritePermission", SecurityUtils.currentUserIsSu());
		return "view-logmanager";
	}

	@PreAuthorize("hasAnyRole('ROLE_SU')")
	@RequestMapping(value = "/logger/{loggerName}/{loggerLevel}", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	public void updateLogLevel(@PathVariable(value = "loggerName") String loggerName,
			@PathVariable(value = "loggerLevel") String loggerLevelStr)
	{
		// SLF4j logging facade does not support runtime change of log level, cast to Logback logger
		org.slf4j.Logger logger = LoggerFactory.getLogger(loggerName);
		if (!(logger instanceof ch.qos.logback.classic.Logger))
		{
			throw new RuntimeException("Root logger is not a Logback logger");
		}
		ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;

		// update log level
		Level loggerLevel;
		try
		{
			loggerLevel = Level.valueOf(loggerLevelStr);
		}
		catch (IllegalArgumentException e)
		{
			throw new RuntimeException("Invalid log level [" + loggerLevelStr + "]");
		}
		logbackLogger.setLevel(loggerLevel);
	}

	@PreAuthorize("hasAnyRole('ROLE_SU')")
	@RequestMapping(value = "/loggers/reset", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	public void resetLoggers()
	{
		ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
		if (!(iLoggerFactory instanceof LoggerContext))
		{
			throw new RuntimeException("Logger factory is not a Logback logger context");
		}
		LoggerContext loggerContext = (LoggerContext) iLoggerFactory;
		ContextInitializer ci = new ContextInitializer(loggerContext);
		URL url = ci.findURLOfDefaultConfigurationFile(true);
		loggerContext.reset();
		try
		{
			ci.configureByResource(url);
		}
		catch (JoranException e)
		{
			LOG.error("Error reloading log configuration", e);
			throw new RuntimeException(e);
		}
	}
}
