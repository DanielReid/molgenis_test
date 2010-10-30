package plugin.login;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import molgenis_test.MolgenisUser;
import molgenis_test.MolgenisUserGroup;

import org.apache.log4j.Logger;
import org.molgenis.framework.db.Database;
import org.molgenis.framework.db.DatabaseException;
import org.molgenis.framework.db.QueryRule;
import org.molgenis.framework.db.QueryRule.Operator;
import org.molgenis.framework.security.Login;
import org.molgenis.util.Entity;

//public class DatabaseLogin implements Login
//{
//	enum Permission
//	{
//		read, edit
//	};
//
//	/** The current use that has been authenticated (if any) */
//	MolgenisUser user;
//	/** The current user groups for this user */
//	List<MolgenisUserGroup> groups;
//	/** Map to quickly retrieve a permission */
//	Map<String, Permission> permissionMap = new TreeMap<String, Permission>();
//	/** for logging */
//	Logger logger = Logger.getLogger(this.getClass().getSimpleName());
//
//	@Override
//	public QueryRule getRowlevelSecurityFilters(Entity entity)
//	{
//		if (entity instanceof RowLevelSecurity)
//		{
//			List<Integer> groupids = new ArrayList<Integer>();
//			for (MolgenisUserGroup g : groups)
//				groupids.add(g.getId());
//
//			QueryRule q1 = new QueryRule("editableBy", Operator.IN, groupids);
//			QueryRule q2 = new QueryRule("viewableBy", Operator.IN, groupids);
//			q2.setOr(true);
//
//			return new QueryRule(q1, q2);
//		}
//
//		return null;
//	}
//
//	@Override
//	public Integer getUserId()
//	{
//		if (user != null) return user.getId();
//		return null;
//	}
//
//	@Override
//	public String getUserName()
//	{
//		if (user != null) return user.getName();
//		return null;
//	}
//
//	@Override
//	public <E extends Entity> boolean canEdit(Class<E> entityClass)
//	{
//		if (user != null && user.getSuperuser()) return true;
//		if (permissionMap.get(entityClass.getName()) != null
//				&& permissionMap.get(entityClass.getName()).equals(Permission.edit))
//		{
//			// logger.debug("Checking edit permission for entity " +
//			// entityClass.getName() + ": true");
//			return true;
//		}
//		// logger.debug("Checking edit permission for entity " +
//		// entityClass.getName() + ": false");
//		return false;
//	}
//
//	@Override
//	public <E extends Entity> boolean readAllowed(Class<E> entityClass)
//	{
//
//		if (user != null && user.getSuperuser()) return true;
//		if (this.editAllowed(entityClass) || permissionMap.get(entityClass.getName()) != null
//				&& permissionMap.get(entityClass.getName()).equals(Permission.read))
//		{
//			// logger.debug("Checking read permission for entity " +
//			// entityClass.getName() + ": true");
//			return true;
//		}
//		// logger.debug("Checking read permission for entity " +
//		// entityClass.getName() + ": false");
//		return false;
//	}
//
//	@Override
//	public boolean isAuthenticated()
//	{
//		return user != null;
//	}
//
//	@Override
//	public boolean login(Database db, String name, String password)
//	{
//		// username is required
//		if (name == null || "".equals(name)) return false;
//		// password is required
//		if (password == null || "".equals(password)) return false;
//
//		try
//		{
//			MolgenisUser example = new MolgenisUser();
//			example.setName(name);
//			example.setPassword(password);
//			example.setSuperuser(null);
//
//			List<MolgenisUser> users = db.findByExample(example);
//			if (users.size() == 1 && users.get(0).getName().equals(name) && users.get(0).getPassword().equals(password))
//			{
//				user = users.get(0);
//				this.reload(db);
//				return true;
//			}
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//		return false;
//	}
//
//	@Override
//	public void logout()
//	{
//		this.user = null;
//		this.permissionMap.clear();
//	}
//
//	@Override
//	public void reload(Database db) throws DatabaseException, ParseException
//	{
//		if (this.user == null) return;
//
//		// // get the groups this user is member of
//		groups = db.query(MolgenisUserGroup.class).equals("members", user.getId()).find();
//
//		// create the permissions map
//		permissionMap = new TreeMap<String, Permission>();
//
//		for (MolgenisUserGroup group : groups)
//		{
//			// get the editable entities
//			if (group.getAllowedToEdit().size() > 0)
//			{
//				List<MolgenisEntity> editableEntities = db.query(MolgenisEntity.class).in("id",
//						group.getAllowedToEdit()).find();
//				for (MolgenisEntity entity : editableEntities)
//				{
//					permissionMap.put(entity.getClassName(), Permission.edit);
//				}
//			}
//			// get the viewable entities
//			if (group.getAllowedToView().size() > 0)
//			{
//				List<MolgenisEntity> viewableEntities = db.query(MolgenisEntity.class).in("id",
//						group.getAllowedToView()).find();
//				for (MolgenisEntity entity : viewableEntities)
//				{
//					// only add if not already editable permission
//					if (permissionMap.get(entity.getClassName()) == null)
//					{
//						permissionMap.put(entity.getClassName(), Permission.read);
//					}
//				}
//			}
//		}
//
//		logger.debug(toString());
//	}
//
//	public String toString()
//	{
//		StringBuffer result = new StringBuffer();
//		result.append("Login(user=" + this.getUserName() + " roles=");
//		for (int i = 0; i < groups.size(); i++)
//		{
//			if (i > 0) result.append("," + groups.get(i).getName());
//			else
//				result.append(groups.get(i).getName());
//		}
//		for (String key : permissionMap.keySet())
//		{
//			result.append(" " + key + "=" + permissionMap.get(key));
//		}
//		result.append(")");
//
//		return result.toString();
//	}
//
//	@Override
//	public boolean isLoginRequired()
//	{
//		return false;
//	}
//
//	@Override
//	public <E extends Entity> boolean editAllowed(E entity) throws DatabaseException
//	{
//		if (entity instanceof RowLevelSecurity)
//		{
//			for (MolgenisUserGroup g : groups)
//			{
//				if (((RowLevelSecurity) entity).getEditableBy().equals(g.getId()))
//				{
//					//logger.debug("Checking read permission for entity " + entity + ": true");
//					return true;
//				}
//			}
//			//logger.debug("Checking read permission for entity " + entity + ": false");
//			return false;
//		}
//		//logger.debug("Checking read permission for entity " + entity + ": true");
//		return true;
//	}
//}
