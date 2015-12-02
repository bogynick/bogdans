/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.PrivilegeNode;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;

public class GrantTask
    implements DataDefinitionTask<Grant>
{
    @Override
    public String getName()
    {
        return "GRANT";
    }

    @Override
    public void execute(Grant statement, Session session, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine)
    {
        QualifiedTableName tableName = createQualifiedTableName(session, statement, statement.getTableName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        accessControl.checkCanGrantTablePrivilege(session.getIdentity(), tableName);

        Set<Privilege> privileges = statement.getPrivilegeNodes().stream()
                .map(PrivilegeNode::getPrivilege)
                .collect(Collectors.toSet());

        Identity identity = statement.getIdentityNode().getIdentity();

        metadata.grantTablePrivileges(session, tableName, privileges, identity, statement.isOption());
    }
}
