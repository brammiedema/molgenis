package org.molgenis.data.annotation;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.annotation.entity.AnnotatorInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created with IntelliJ IDEA. User: charbonb Date: 21/02/14 Time: 11:24 To change this template use File | Settings |
 * File Templates.
 */
public abstract class AbstractRepositoryAnnotator implements RepositoryAnnotator
{
	@Autowired
	AnnotationService annotatorService;

	@Override
	public String canAnnotate(EntityMetaData repoMetaData)
	{
		Iterable<AttributeMetaData> annotatorAttributes = getInputMetaData();
		for (AttributeMetaData annotatorAttribute : annotatorAttributes)
		{
			// one of the needed attributes not present? we can not annotate
			if (repoMetaData.getAttribute(annotatorAttribute.getName()) == null)
			{
				return "missing required attribute";
			}

			// one of the needed attributes not of the correct type? we can not annotate
			if (!repoMetaData.getAttribute(annotatorAttribute.getName()).getDataType()
					.equals(annotatorAttribute.getDataType()))
			{
				return "a required attribute has the wrong datatype";
			}

			// Are the runtime property files not available, or is a webservice down? we can not annotate
			if (!annotationDataExists())
			{
				return "annotation datasource unreachable";
			}
		}

		return "true";
	}

	/**
	 * Checks if folder and files that were set with a runtime property actually exist, or if a webservice can be
	 * reached
	 *
	 * @return boolean
	 */
	protected abstract boolean annotationDataExists();

	@Override
	@Transactional
	public Iterator<Entity> annotate(final Iterable<Entity> sourceIterable)
	{
		Iterator<Entity> source = sourceIterable.iterator();
		return new Iterator<Entity>()
		{
			int current = 0;
			int size = 0;
			List<Entity> results;
			Entity result;

			@Override
			public boolean hasNext()
			{
				return current < size || source.hasNext();
			}

			@Override
			public Entity next()
			{
				Entity sourceEntity = null;
				if (current >= size)
				{
					if (source.hasNext())
					{
						try
						{
							sourceEntity = source.next();
							results = annotateEntity(sourceEntity);
						}
						catch (IOException e)
						{
							throw new RuntimeException(e);
						}
						catch (InterruptedException e)
						{
							throw new RuntimeException(e);
						}

						size = results.size();
					}
					current = 0;
				}
				if (results.size() > 0)
				{
					result = results.get(current);
				}
				else
				{
					result = sourceEntity;
				}
				++current;
				return result;
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	public abstract List<Entity> annotateEntity(Entity entity) throws IOException, InterruptedException;

	@Override
	public String getFullName()
	{
		return RepositoryAnnotator.ANNOTATOR_PREFIX + getSimpleName();
	}

	@Override
	public String getDescription()
	{
		String desc = "TODO";
		AnnotatorInfo annotatorInfo = getInfo();
		if(annotatorInfo != null) desc = annotatorInfo.getDescription();
		return desc;
	}

}
