/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.core;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Base class for all model-based tests.
 * 
 * Extend this class to create a state machine model for testing.
 * The ModelEngine will discover @ModelAction methods and @ModelVariable fields
 * via reflection, then auto-explore valid execution paths.
 * 
 * Usage:
 * <pre>
 * public class ConnectionModel extends Model {
 *     
 *     &#64;ModelVariable
 *     private boolean closed = false;
 *     
 *     &#64;ModelVariable
 *     private boolean autoCommit = true;
 *     
 *     private Connection connection;
 *     
 *     public ConnectionModel(Connection conn) {
 *         this.connection = conn;
 *     }
 *     
 *     &#64;ModelAction(weight = 10)
 *     &#64;ModelRequirement(variable = "closed", value = "false")
 *     public void close() throws SQLException {
 *         connection.close();
 *         closed = true;
 *     }
 *     
 *     &#64;ModelAction(weight = 5)
 *     &#64;ModelRequirement(variable = "closed", value = "false")
 *     &#64;ModelRequirement(variable = "autoCommit", value = "false")
 *     public void commit() throws SQLException {
 *         connection.commit();
 *     }
 * }
 * </pre>
 */
public abstract class Model {
    
    private static final Logger LOGGER = Logger.getLogger(Model.class.getName());
    
    /** Cache of discovered actions */
    private List<ActionInfo> actions;
    
    /** Cache of discovered variables */
    private Map<String, VariableInfo> variables;
    
    /** Name of this model (for logging) */
    private String modelName;
    
    /** Whether model has been initialized */
    private boolean initialized = false;
    
    /**
     * Default constructor.
     */
    protected Model() {
        this.modelName = this.getClass().getSimpleName();
    }
    
    /**
     * Constructor with custom name.
     * 
     * @param name the model name
     */
    protected Model(String name) {
        this.modelName = name;
    }
    
    /**
     * Initializes the model by discovering actions and variables via reflection.
     * Called automatically by ModelEngine before execution.
     */
    public void initialize() {
        if (initialized) {
            return;
        }
        
        discoverVariables();
        discoverActions();
        initialized = true;
        
        LOGGER.fine(() -> String.format(
            "Model '%s' initialized with %d actions and %d variables",
            modelName, actions.size(), variables.size()
        ));
    }
    
    /**
     * Discovers all @ModelVariable annotated fields.
     */
    private void discoverVariables() {
        variables = new HashMap<>();
        Class<?> clazz = this.getClass();
        
        while (clazz != null && clazz != Model.class) {
            for (Field field : clazz.getDeclaredFields()) {
                ModelVariable annotation = field.getAnnotation(ModelVariable.class);
                if (annotation != null) {
                    field.setAccessible(true);
                    String name = annotation.name().isEmpty() ? field.getName() : annotation.name();
                    variables.put(name, new VariableInfo(field, annotation, name));
                }
            }
            clazz = clazz.getSuperclass();
        }
    }
    
    /**
     * Discovers all @ModelAction annotated methods.
     */
    private void discoverActions() {
        actions = new ArrayList<>();
        Class<?> clazz = this.getClass();
        
        while (clazz != null && clazz != Model.class) {
            for (Method method : clazz.getDeclaredMethods()) {
                ModelAction annotation = method.getAnnotation(ModelAction.class);
                if (annotation != null) {
                    method.setAccessible(true);
                    
                    // Get requirements
                    List<RequirementInfo> requirements = new ArrayList<>();
                    ModelRequirement[] reqAnnotations = method.getAnnotationsByType(ModelRequirement.class);
                    for (ModelRequirement req : reqAnnotations) {
                        requirements.add(new RequirementInfo(req));
                    }
                    
                    actions.add(new ActionInfo(method, annotation, requirements));
                }
            }
            clazz = clazz.getSuperclass();
        }
    }
    
    /**
     * Gets all discovered actions.
     * 
     * @return list of action info objects
     */
    public List<ActionInfo> getActions() {
        if (!initialized) {
            initialize();
        }
        return actions;
    }
    
    /**
     * Gets all discovered variables.
     * 
     * @return map of variable name to info
     */
    public Map<String, VariableInfo> getVariables() {
        if (!initialized) {
            initialize();
        }
        return variables;
    }
    
    /**
     * Gets the current value of a state variable.
     * 
     * @param variableName name of the variable
     * @return current value as Object
     */
    public Object getVariableValue(String variableName) {
        VariableInfo info = variables.get(variableName);
        if (info == null) {
            throw new IllegalArgumentException("Unknown variable: " + variableName);
        }
        try {
            return info.field.get(this);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot access variable: " + variableName, e);
        }
    }
    
    /**
     * Gets the current value of a state variable as a String.
     * 
     * @param variableName name of the variable
     * @return current value as String
     */
    public String getVariableValueAsString(String variableName) {
        Object value = getVariableValue(variableName);
        return value == null ? "null" : value.toString();
    }
    
    /**
     * Checks if an action's requirements are currently satisfied.
     * 
     * @param action the action to check
     * @return true if all requirements are met
     */
    public boolean areRequirementsMet(ActionInfo action) {
        for (RequirementInfo req : action.requirements) {
            String actualValue = getVariableValueAsString(req.variable);
            if (!evaluateRequirement(req, actualValue)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Evaluates a single requirement.
     */
    private boolean evaluateRequirement(RequirementInfo req, String actualValue) {
        String expectedValue = req.expectedValue;
        
        switch (req.operator) {
            case EQUALS:
                return actualValue.equalsIgnoreCase(expectedValue);
            case NOT_EQUALS:
                return !actualValue.equalsIgnoreCase(expectedValue);
            case GREATER_THAN:
                return Double.parseDouble(actualValue) > Double.parseDouble(expectedValue);
            case GREATER_THAN_OR_EQUAL:
                return Double.parseDouble(actualValue) >= Double.parseDouble(expectedValue);
            case LESS_THAN:
                return Double.parseDouble(actualValue) < Double.parseDouble(expectedValue);
            case LESS_THAN_OR_EQUAL:
                return Double.parseDouble(actualValue) <= Double.parseDouble(expectedValue);
            default:
                return false;
        }
    }
    
    /**
     * Gets list of actions that are currently valid to execute.
     * 
     * @return list of valid actions
     */
    public List<ActionInfo> getValidActions() {
        List<ActionInfo> valid = new ArrayList<>();
        for (ActionInfo action : actions) {
            if (areRequirementsMet(action)) {
                valid.add(action);
            }
        }
        return valid;
    }
    
    /**
     * Executes an action.
     * 
     * @param action the action to execute
     * @throws Exception if action throws
     */
    public void executeAction(ActionInfo action) throws Exception {
        try {
            action.method.invoke(this);
            action.callCount++;
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }
    
    /**
     * Gets the model name.
     * 
     * @return model name
     */
    public String getModelName() {
        return modelName;
    }
    
    /**
     * Resets all call counts (for multiple runs).
     */
    public void resetCallCounts() {
        for (ActionInfo action : actions) {
            action.callCount = 0;
        }
    }
    
    /**
     * Optional setup method called before each model run.
     * Override to initialize resources.
     */
    public void setup() throws Exception {
        // Override in subclass
    }
    
    /**
     * Optional teardown method called after each model run.
     * Override to cleanup resources.
     */
    public void teardown() throws Exception {
        // Override in subclass
    }
    
    /**
     * Gets a state snapshot for logging.
     * 
     * @return map of variable names to values
     */
    public Map<String, String> getStateSnapshot() {
        Map<String, String> state = new HashMap<>();
        for (Map.Entry<String, VariableInfo> entry : variables.entrySet()) {
            state.put(entry.getKey(), getVariableValueAsString(entry.getKey()));
        }
        return state;
    }
    
    // Inner classes for caching reflection info
    
    /**
     * Cached info about an action method.
     */
    public static class ActionInfo {
        public final Method method;
        public final String name;
        public final int weight;
        public final boolean callFirst;
        public final boolean callLast;
        public final int callLimit;
        public final boolean callOnce;
        public final List<RequirementInfo> requirements;
        public int callCount = 0;
        
        ActionInfo(Method method, ModelAction annotation, List<RequirementInfo> requirements) {
            this.method = method;
            this.name = method.getName();
            this.weight = annotation.weight();
            this.callFirst = annotation.callFirst();
            this.callLast = annotation.callLast();
            this.callLimit = annotation.callLimit();
            this.callOnce = annotation.callOnce();
            this.requirements = requirements;
        }
        
        @Override
        public String toString() {
            return String.format("Action[%s, weight=%d, calls=%d]", name, weight, callCount);
        }
    }
    
    /**
     * Cached info about a state variable.
     */
    public static class VariableInfo {
        public final Field field;
        public final String name;
        public final boolean trace;
        
        VariableInfo(Field field, ModelVariable annotation, String name) {
            this.field = field;
            this.name = name;
            this.trace = annotation.trace();
        }
    }
    
    /**
     * Cached info about a requirement.
     */
    public static class RequirementInfo {
        public final String variable;
        public final String expectedValue;
        public final ModelRequirement.Operator operator;
        public final boolean throwsOnFailure;
        public final Class<? extends Exception> expectedException;
        
        RequirementInfo(ModelRequirement annotation) {
            this.variable = annotation.variable();
            this.expectedValue = annotation.value();
            this.operator = annotation.operator();
            this.throwsOnFailure = annotation.throwsOnFailure();
            this.expectedException = annotation.expectedException();
        }
    }
}
