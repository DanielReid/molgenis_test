<!--Date:        March 9, 2009
 * Template:	PluginScreenFTLTemplateGen.ftl.ftl
 * generator:   org.molgenis.generate.screen.PluginScreenFTLTemplateGen 3.2.0-testing
 * 
 * THIS FILE IS A TEMPLATE. PLEASE EDIT :-)
-->
<#macro org_molgenis_plugins_examples_file_DownloadPlugin screen>
<!-- normally you make one big form for the whole plugin-->
<form method="post" enctype="multipart/form-data" name="${screen.name}">
	<!--needed in every form: to redirect the request to the right screen-->
	<input type="hidden" name="__target" value="${screen.name}"" />
	<!--needed in every form: to define the action. This can be set by the submit button-->
	<input type="hidden" name="__action" />
	
<!-- this shows a title and border -->
	<div class="formscreen">
		<div class="form_header" id="${screen.getName()}">
		${screen.label}
		</div>
		
		<#--optional: mechanism to show messages-->
		<#list screen.getMessages() as message>
			<#if message.success>
		<p class="successmessage">${message.text}</p>
			<#else>
		<p class="errormessage">${message.text}</p>
			</#if>
		</#list>
		
		<div class="screenbody">
			<div class="screenpadding">	
<#--begin your plugin-->	

<!--setting the standard MOLGENIS action to 'download' which forces a download-->
<!--notice how we use GET instead of POST by passing parameters using a simple URL instead of form-->

<#list screen.fileEntities as e>
	<a href="?__target=${screen.name}&__filename=${e.normalFile}&__action=download">${e.id}</a>
</#list>
	
<#--end of your plugin-->	
			</div>
		</div>
	</div>
</form>
</#macro>
