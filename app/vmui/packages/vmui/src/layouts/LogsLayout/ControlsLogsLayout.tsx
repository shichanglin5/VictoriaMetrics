import React, { FC } from "preact/compat";
import classNames from "classnames";
import GlobalSettings from "../../components/Configurators/GlobalSettings/GlobalSettings";
import { ControlsProps } from "../Header/HeaderControls/HeaderControls";
import { TimeSelector } from "../../components/Configurators/TimeRangeSettings/TimeSelector/TimeSelector";
import TenantsFields from "../../components/Configurators/GlobalSettings/TenantsConfiguration/TenantsFields";

const ControlsLogsLayout: FC<ControlsProps> = ({ isMobile }) => {

  return (
    <div
      className={classNames({
        "vm-header-controls": true,
        "vm-header-controls_mobile": isMobile,
      })}
    >
      <TenantsFields/>
      <TimeSelector/>
      <GlobalSettings/>
    </div>
  );
};

export default ControlsLogsLayout;
