package org.molgenis.data.annotation.entity.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.annotation.entity.ResultFilter;
import org.molgenis.data.support.DefaultAttributeMetaData;
import org.molgenis.data.support.DefaultEntityMetaData;
import org.molgenis.data.support.MapEntity;
import org.molgenis.data.vcf.VcfRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.FluentIterable;

public class ExacResultFilter implements ResultFilter
{
	private List<AttributeMetaData> attributes;
	private static final Logger LOG = LoggerFactory.getLogger(ExacResultFilter.class);
	
	public ExacResultFilter(List<AttributeMetaData> attributes){
		this.attributes = attributes;
	}
	@Override
	public Collection<AttributeMetaData> getRequiredAttributes()
	{
		return Arrays.asList(VcfRepository.REF_META, VcfRepository.ALT_META);
	}
	

	@Override
	public com.google.common.base.Optional<Entity> filterResults(Iterable<Entity> results, Entity annotatedEntity)
	{
	List<Entity> newEntities = new ArrayList<Entity>();
	
	DefaultEntityMetaData entityMetadata =  new DefaultEntityMetaData("exac");
	entityMetadata.addAttribute("ID").setIdAttribute(true);
	entityMetadata.addAttribute("REF");
	entityMetadata.addAttribute("ALT");
	entityMetadata.addAttribute("#CHROM");
	entityMetadata.addAttribute("POS");
	
	for(AttributeMetaData attribute : this.attributes){
		entityMetadata.addAttributeMetaData(attribute);
	}
	// value header
	//##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence type as predicted by VEP. Format: 
	//Allele|Gene|Feature|Feature_type|Consequence|cDNA_position|CDS_position|Protein_position|Amino_acids| #9
	//Codons|Existing_variation|ALLELE_NUM|DISTANCE|STRAND|SYMBOL|SYMBOL_SOURCE|HGNC_ID|BIOTYPE|CANONICAL| #10
	//CCDS|ENSP|SWISSPROT|TREMBL|UNIPARC|SIFT|PolyPhen|EXON|INTRON|DOMAINS|HGVSc|HGVSp|GMAF|AFR_MAF|AMR_MAF| #15
	//ASN_MAF|EUR_MAF|AA_MAF|EA_MAF|CLIN_SIG|SOMATIC|PUBMED|MOTIF_NAME|MOTIF_POS|HIGH_INF_POS|MOTIF_SCORE_CHANGE|LoF_info|LoF_flags|LoF_filter|LoF"> #15
	
	// value
	//CSQ=C|ENSG00000223972|ENST00000456328|Transcript|non_coding_transcript_exon_variant&non_coding_transcript_variant|620||||||1||1|DDX11L1|HGNC|37102|
	//processed_transcript|YES||||||||3/3|||ENST00000456328.2:n.620G>C|||||||||||||||||||,C|ENSG00000223972|ENST00000450305|
	//Transcript|splice_region_variant&non_coding_transcript_exon_variant&non_coding_transcript_variant|412||||||1||1|DDX11L1|
	//HGNC|37102|transcribed_unprocessed_pseudogene|||||||||5/6|||ENST00000450305.2:n.412G>C|||||||||||||||||||,C|ENSG00000223972|
	//ENST00000515242|Transcript|non_coding_transcript_exon_variant&non_coding_transcript_variant|613||||||1||1|DDX11L1|HGNC|37102|
	//transcribed_unprocessed_pseudogene|||||||||3/3|||ENST00000515242.2:n.613G>C|||||||||||||||||||,C|ENSG00000223972|ENST00000518655|
	//Transcript|intron_variant&non_coding_transcript_variant|||||||1||1|DDX11L1|HGNC|37102|transcribed_unprocessed_pseudogene||||||||||
	//2/3||ENST00000518655.2:n.482-31G>C|||||||||||||||||||,C||ENSR00000528767|RegulatoryFeature|regulatory_region_variant|||||||1||||||
	//regulatory_region|||||||||||||||||||||||||||||||
	for(Entity entity : results){
		String csq = entity.getString("INFO_CSQ");
		LOG.info("CSQ: : : : : " + csq);
		MapEntity newEntity = new MapEntity(entityMetadata);
		String[] splittedCsq = csq.split("|");
		// interested in the GMAF|AFR_MAF|AMR_MAF|ASN_MAF|EUR_MAF|AA_MAF|EA_MAF fields within the CSQ field in the infofield of the exac v0.3 fields
		newEntity.set("ID", entity.getIdValue());
		String chrom = entity.getString("#CHROM");
		String pos = entity.getString("POS");
		String ref = entity.getString("REF");
		String alt = entity.getString("ALT");
		
		String ac = entity.getString("INFO_AC");
		String gMaf = splittedCsq[31];
		String afrMaf = splittedCsq[32];
		String amrMaf = splittedCsq[33];
		String asnMaf = splittedCsq[34];
		String eurMaf = splittedCsq[35];
		String aaMaf = splittedCsq[36];
		String eaMaf = splittedCsq[37];
		
		newEntity.set("#CHROM", chrom);
		newEntity.set("POS", pos);
		newEntity.set("REF", ref);
		newEntity.set("ALT", alt);
		
		newEntity.set("EXAC_GMAF", gMaf );
		newEntity.set("EXAC_AFR_MAF", afrMaf );
		newEntity.set("EXAC_AMR_MAF", amrMaf );
		newEntity.set("EXAC_ASN_MAF", asnMaf );
		newEntity.set("EXAC_EUR_MAF", eurMaf );
		newEntity.set("EXAC_AA_MAF", aaMaf );
		newEntity.set("EXAC_EA_MAF", eaMaf );
		newEntity.set("EXAC_AC", ac );

		newEntities.add(newEntity);
	} 
	
	
	return FluentIterable.from(newEntities).filter(result -> StringUtils.equals(result.getString(VcfRepository.REF),
			annotatedEntity.getString(VcfRepository.REF))
			&& StringUtils.equals(result.getString(VcfRepository.ALT),
					annotatedEntity.getString(VcfRepository.ALT))).first();
	}

}
