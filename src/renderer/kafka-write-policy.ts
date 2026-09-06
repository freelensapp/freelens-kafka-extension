export interface WriteActionConfirmationState {
  confirmationAccepted: boolean;
  requiredResourceName?: string;
  enteredResourceName?: string;
}

export function canSubmitWriteAction({
  confirmationAccepted,
  requiredResourceName,
  enteredResourceName = "",
}: WriteActionConfirmationState): boolean {
  if (!confirmationAccepted) return false;
  if (!requiredResourceName) return true;

  return enteredResourceName === requiredResourceName;
}

export function getWriteConfirmationLabel({
  destructive,
  resourceName,
}: {
  destructive: boolean;
  resourceName: string;
}): string {
  if (destructive) return `Type ${resourceName} to confirm`;
  return "Confirm once";
}
