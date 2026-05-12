export const removeUndefinedFields = (
  obj: Record<any, any>,
): Record<any, any> => {
  const newObj: Record<any, any> = {};
  for (const key in obj) {
    if (obj[key] !== undefined) {
      newObj[key] = obj[key];
    }
  }
  return newObj;
};
