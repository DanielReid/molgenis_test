<!--Date:        December 3, 2008
 * Template:	PluginScreenFTLTemplateGen.ftl.ftl
 * generator:   org.molgenis.generate.screen.PluginScreenFTLTemplateGen 3.0.3
 * 
 * THIS FILE IS A TEMPLATE. PLEASE EDIT :-)
-->
<#macro plugins_auth_UserLogin screen>
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
		<#--messages-->
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

<#assign login = screen.login/>
<#assign form = screen.inputs/>
<#if login.authenticated>
	You are logged in as '${login.userName}'.
	${form.logout}
<#else>
	<table>
	<tr><td><label>Name:</label></td><td>${form.name}</td></tr> 
	<tr><td><label>Password:</label></td><td>${form.password}</td></tr>
	</table>
	<br/> ${form.login}
</#if>

<#--end of your plugin-->	
			</div>
		</div>
	</div>
</form>
</#macro>
