package org.molgenis.plugins;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import molgenis_test.MolgenisPermission;
import molgenis_test.MolgenisUser;
import molgenis_test.MolgenisUserGroup;

import org.apache.log4j.Logger;
import org.molgenis.framework.db.Database;
import org.molgenis.framework.db.DatabaseException;
import org.molgenis.framework.db.QueryRule;
import org.molgenis.framework.security.Login;
import org.molgenis.util.Entity;

public class DatabaseLogin<E extends Entity> implements Login<E>
{
	enum Permission
	{
		read, edit
	};

	/** The current use that has been authenticated (if any) */
	MolgenisUser user;
	/** The groups current user is part of */
	List<MolgenisUserGroup> groups;
	/** If the current user is super user */
	Boolean superuser = false;
	/** Map to quickly retrieve a permission */
	Map<String, Permission> permissionMap = new TreeMap<String, Permission>();
	/** for logging */
	Logger logger = Logger.getLogger(this.getClass().getSimpleName());

	public DatabaseLogin()
	{
	}

	@Override
	public QueryRule getRowlevelSecurityFilters(Class<E> klazz)
	{
		return null;
	}

	@Override
	public Integer getUserId()
	{
		if (user != null)
			return user.getId();
		return null;
	}

	@Override
	public String getUserName()
	{
		if (user != null)
			return user.getName();
		return null;
	}

	@Override
	public boolean canWrite(Class<E> entityClass)
	{
		// logger.debug("Checking edit permission for entity " +
		// entityClass.getName());
		if (this.superuser)
			return true;
		if (permissionMap.get(entityClass.getName()) != null
				&& permissionMap.get(entityClass.getName()).equals(Permission.edit))
		{
			logger.debug("true");
			return true;
		}
		return false;
	}

	@Override
	public boolean canRead(Class<E> entityClass)
	{
		logger.debug("Checking read permission for entity " + entityClass.getName());
		if (this.superuser)
			return true;
		if (this.canWrite(entityClass) || permissionMap.get(entityClass.getName()) != null
				&& permissionMap.get(entityClass.getName()).equals(Permission.read))
		{
			// logger.debug("true");
			return true;
		}
		return false;
	}

	@Override
	public boolean isAuthenticated()
	{
		return user != null;
	}

	@Override
	public boolean login(Database db, String name, String password)
	{
		// username is required
		if (name == null || "".equals(name))
			return false;
		// password is required
		if (password == null || "".equals(password))
			return false;

		try
		{
			MolgenisUser example = new MolgenisUser();
			example.setName(name);
			example.setPassword(password);
			List<MolgenisUser> users = db.findByExample(example);
			if (users.size() == 1 && users.get(0).getName().equals(name) && users.get(0).getPassword().equals(password))
			{
				user = users.get(0);
				this.reload(db);
				return true;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public void logout()
	{
		this.user = null;
		this.permissionMap.clear();
	}

	@Override
	public void reload(Database db) throws DatabaseException, ParseException
	{
		if (this.user == null)
			return;

		// get the groups this user is member of
		groups = db.query(MolgenisUserGroup.class).equals("members", user.getId()).find();

		// create the permissions map
		permissionMap = new TreeMap<String, Permission>();

		for (MolgenisUserGroup group : groups)
		{
			// get the permissions for this group
			List<MolgenisPermission> permissions = db.query(MolgenisPermission.class).equals("group", group.getId())
					.find();

			for (MolgenisPermission permission : permissions)
			{
				if (permission.getCanEdit())
					permissionMap.put(permission.getMolgenisEntity(), Permission.edit);
				else if (permission.getCanRead() && permissionMap.get(permission.getMolgenisEntity()) != null)
					permissionMap.put(permission.getMolgenisEntity(), Permission.read);

			}
		}

		logger.debug(toString());
	}

	public String toString()
	{
		StringBuffer result = new StringBuffer();
		result.append("Login(user=" + this.getUserName() + " groups=");
		for (int i = 0; i < groups.size(); i++)
		{
			if (i > 0)
				result.append("," + groups.get(i).getName());
			else
				result.append(groups.get(i).getName());
		}
		// for (String key : permissionMap.keySet())
		// {
		// result.append(" " + key + "=" + permissionMap.get(key));
		// }
		result.append(")");

		return result.toString();
	}

	@Override
	public boolean isLoginRequired()
	{
		return false;
	}

	@Override
	public boolean canWrite(E entity) throws DatabaseException
	{
		// TODO Auto-generated method stub
		return true;
	}

	public MolgenisUser getMolgenisUser()
	{
		return this.user;
	}

	@Override
	public boolean canRead(Entity entity) throws DatabaseException
	{
		// TODO Auto-generated method stub
		return false;
	}
}
